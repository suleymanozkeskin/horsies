//! Where the worker process and its children run (Python
//! `core/worker/cpu_placement.py`).
//!
//! With the profile setting `parent_cpu = OWN_CORE` (Linux), the worker
//! process runs on one physical core of the CPUs it may use, both SMT
//! threads of it, and every child on the other CPUs. The worker process
//! dispatches every task and writes every outcome; a child on the other
//! thread of its core slows it. With `SHARED` nothing is placed.
//!
//! - [`plan_own_core`] reads the CPUs that the worker may use and their SMT
//!   siblings, and splits them ([`split_own_core`]). The worker maps
//!   `SHARED` to [`CpuPlacement::SharedCpus`] and `OWN_CORE` to the plan.
//! - [`place_process`] sets the CPUs of a child by its pid. A slot calls it
//!   after it starts a child and before it sends the init frame: the child
//!   waits for the init frame before it starts a thread, so every thread of
//!   the child gets the same CPUs.
//! - [`place_worker_threads`] sets the CPUs of every thread of the worker
//!   process after the first generation. A thread that a placed thread
//!   starts later gets the same CPUs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::worker::executor::protocol::ProcessId;

/// Where Linux lists the SMT siblings of each CPU
/// (`cpu<N>/topology/thread_siblings_list`).
pub const CPU_TOPOLOGY_DIR: &str = "/sys/devices/system/cpu";
/// One entry per thread of this process, named by its thread id.
pub const TASK_DIR: &str = "/proc/self/task";
/// The highest CPU id that a CPU list may name. A larger one is a list that
/// this module cannot read.
pub const MAX_CPU_ID: u32 = 65_535;
/// The most threads of the worker process that one placement sets. A
/// listing with more is refused.
pub const MAX_WORKER_THREADS: usize = 4_096;
/// The longest sibling list that is read, in bytes.
const MAX_SIBLING_LIST_BYTES: u64 = 4_096;

/// A set of CPU ids: not empty, each at most [`MAX_CPU_ID`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CpuIds(BTreeSet<u32>);

impl CpuIds {
    /// `None` when `ids` is empty or names a CPU above [`MAX_CPU_ID`].
    pub fn new(ids: BTreeSet<u32>) -> Option<Self> {
        let in_bounds = ids.last().is_some_and(|last| *last <= MAX_CPU_ID);
        match in_bounds {
            true => Some(Self(ids)),
            false => None,
        }
    }

    pub fn ids(&self) -> &BTreeSet<u32> {
        &self.0
    }

    /// The lowest CPU of the set.
    pub fn lowest(&self) -> u32 {
        *self.0.first().expect("a set of CPU ids is not empty")
    }
}

impl fmt::Display for CpuIds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ids: Vec<String> = self.0.iter().map(u32::to_string).collect();
        write!(f, "[{}]", ids.join(", "))
    }
}

/// The CPUs of the worker process and of its children: disjoint, and each
/// holds at least one CPU. Only [`split_own_core`] builds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnCore {
    parent: CpuIds,
    children: CpuIds,
}

impl OwnCore {
    pub fn parent(&self) -> &CpuIds {
        &self.parent
    }

    pub fn children(&self) -> &CpuIds {
        &self.children
    }
}

/// Where the worker process and its children run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CpuPlacement {
    /// No placement: every process keeps the CPUs it inherits.
    SharedCpus,
    OwnCore(OwnCore),
}

impl CpuPlacement {
    /// The CPUs of each child of this placement.
    pub fn child_cpus(&self) -> ChildCpus {
        match self {
            Self::SharedCpus => ChildCpus::Inherited,
            Self::OwnCore(own) => ChildCpus::Set(own.children.clone()),
        }
    }
}

/// The CPUs of a child, a setting of each pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChildCpus {
    /// The child keeps the CPUs it inherits.
    Inherited,
    /// The child runs on these CPUs, set before its init frame.
    Set(CpuIds),
}

/// The CPUs that the worker may use hold fewer than 2 physical cores.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "parent_cpu=OWN_CORE needs at least 2 physical cores in the CPUs that the worker may use; \
     CPUs {allowed} hold {cores}"
)]
pub struct TooFewCores {
    pub allowed: CpuIds,
    pub cores: usize,
}

/// The sibling list of a CPU could not be read or parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the SMT siblings of CPU {cpu} could not be read: {cause}")]
pub struct TopologyUnreadable {
    pub cpu: u32,
    pub cause: String,
}

/// Why `OWN_CORE` cannot be planned on this host. The worker start fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementRefused {
    #[error("parent_cpu=OWN_CORE runs on Linux only; this platform is {platform}")]
    NotLinux { platform: &'static str },
    #[error("the CPUs that the worker may use could not be read: {cause}")]
    AllowedUnreadable { cause: String },
    #[error(transparent)]
    TooFewCores(TooFewCores),
    #[error(transparent)]
    TopologyUnreadable(TopologyUnreadable),
}

/// The OS refused to set the CPUs of a process.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the CPUs of child {pid} could not be set: {cause}")]
pub struct PlaceRefused {
    pub pid: u32,
    pub cause: String,
}

/// Why the threads of the worker process were not all placed. The worker
/// start fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WorkerNotPlaced {
    #[error("the threads of the worker process could not be listed in {dir}: {cause}")]
    ThreadsUnlisted { dir: String, cause: String },
    #[error("the worker process has more than {MAX_WORKER_THREADS} threads to place")]
    TooManyThreads,
    #[error("the CPUs of worker thread {thread_id} could not be set: {cause}")]
    ThreadRefused { thread_id: u32, cause: String },
}

/// A text that is not a CPU list.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a CPU list: {text:?}")]
pub struct NotACpuList {
    pub text: String,
}

/// Where the threads of the worker process are listed: one entry per
/// thread, named by its thread id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerThreads {
    dir: PathBuf,
}

impl WorkerThreads {
    /// Every thread of this process (`/proc/self/task`).
    pub fn of_this_process() -> Self {
        Self {
            dir: PathBuf::from(TASK_DIR),
        }
    }

    /// The threads listed in `dir`, for a test that places only threads it
    /// owns.
    #[cfg(test)]
    pub fn listed_in(dir: PathBuf) -> Self {
        Self { dir }
    }
}

/// The CPUs of a Linux CPU list such as `0-3,8-11` or `6,7`. Pure.
pub fn parse_cpu_list(text: &str) -> Result<CpuIds, NotACpuList> {
    let refused = || NotACpuList {
        text: text.to_owned(),
    };
    let mut ids = BTreeSet::new();
    for part in text.trim().split(',') {
        let (first, last) = match part.split_once('-') {
            Some((first, last)) => (first, Some(last)),
            None => (part, None),
        };
        let low = cpu_number(first).ok_or_else(refused)?;
        let high = match last {
            Some(last) => cpu_number(last).ok_or_else(refused)?,
            None => low,
        };
        if high < low {
            return Err(refused());
        }
        ids.extend(low..=high);
    }
    CpuIds::new(ids).ok_or_else(refused)
}

/// A CPU number of a list: ASCII digits, at most [`MAX_CPU_ID`].
fn cpu_number(text: &str) -> Option<u32> {
    match !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        true => text
            .parse::<u32>()
            .ok()
            .filter(|number| *number <= MAX_CPU_ID),
        false => None,
    }
}

/// Split `allowed` into the core of the worker process and the CPUs of
/// the children. Pure. A physical core is a sibling set cut to `allowed`.
/// The worker process takes the core of the lowest allowed CPU; the
/// children take every other allowed CPU. `siblings` holds every CPU of
/// `allowed`; a CPU missing from it is a defect of the caller.
pub fn split_own_core(
    allowed: &CpuIds,
    siblings: &BTreeMap<u32, CpuIds>,
) -> Result<OwnCore, TooFewCores> {
    let core_of = |cpu: u32| -> BTreeSet<u32> {
        let listed = siblings
            .get(&cpu)
            .unwrap_or_else(|| panic!("the siblings of allowed CPU {cpu} are missing"));
        listed.ids().intersection(allowed.ids()).copied().collect()
    };
    let cores: BTreeSet<BTreeSet<u32>> = allowed.ids().iter().map(|cpu| core_of(*cpu)).collect();
    if cores.len() < 2 {
        return Err(TooFewCores {
            allowed: allowed.clone(),
            cores: cores.len(),
        });
    }
    let parent = core_of(allowed.lowest());
    let children: BTreeSet<u32> = allowed.ids().difference(&parent).copied().collect();
    match (CpuIds::new(parent), CpuIds::new(children)) {
        (Some(parent), Some(children)) => Ok(OwnCore { parent, children }),
        (parent, children) => panic!(
            "two physical cores split into an empty side: parent {parent:?}, children {children:?}"
        ),
    }
}

/// The SMT siblings of each CPU of `allowed`, from `topology_dir`.
pub fn read_siblings(
    allowed: &CpuIds,
    topology_dir: &Path,
) -> Result<BTreeMap<u32, CpuIds>, TopologyUnreadable> {
    let mut siblings = BTreeMap::new();
    for cpu in allowed.ids() {
        let cpu = *cpu;
        let path = topology_dir
            .join(format!("cpu{cpu}"))
            .join("topology")
            .join("thread_siblings_list");
        let text = read_bounded(&path).map_err(|cause| TopologyUnreadable { cpu, cause })?;
        let listed = parse_cpu_list(&text).map_err(|error| TopologyUnreadable {
            cpu,
            cause: error.to_string(),
        })?;
        if !listed.ids().contains(&cpu) {
            return Err(TopologyUnreadable {
                cpu,
                cause: format!("its list {listed} omits it"),
            });
        }
        let _replaced = siblings.insert(cpu, listed);
    }
    Ok(siblings)
}

/// The text of a small file, at most [`MAX_SIBLING_LIST_BYTES`].
fn read_bounded(path: &Path) -> Result<String, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let mut text = String::new();
    let _read = file
        .take(MAX_SIBLING_LIST_BYTES)
        .read_to_string(&mut text)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    Ok(text)
}

/// The placement of `OWN_CORE` on this host: the CPUs that this process
/// may use, split by [`split_own_core`]. Reads the host; changes nothing.
pub fn plan_own_core() -> Result<OwnCore, PlacementRefused> {
    let allowed = allowed_cpus()?;
    let siblings = read_siblings(&allowed, Path::new(CPU_TOPOLOGY_DIR))
        .map_err(PlacementRefused::TopologyUnreadable)?;
    split_own_core(&allowed, &siblings).map_err(PlacementRefused::TooFewCores)
}

/// Set the CPUs of process `pid`. Threads that it starts later get the
/// same CPUs.
pub fn place_process(pid: ProcessId, cpus: &CpuIds) -> Result<(), PlaceRefused> {
    os::set_cpus(pid.as_pid_t(), cpus).map_err(|cause| PlaceRefused {
        pid: pid.get(),
        cause,
    })
}

/// Set the CPUs of every thread of `threads`; return how many. A thread
/// that ended between the listing and the call is skipped.
pub fn place_worker_threads(
    cpus: &CpuIds,
    threads: &WorkerThreads,
) -> Result<usize, WorkerNotPlaced> {
    let unlisted = |cause: String| WorkerNotPlaced::ThreadsUnlisted {
        dir: threads.dir.display().to_string(),
        cause,
    };
    let entries = std::fs::read_dir(&threads.dir).map_err(|error| unlisted(error.to_string()))?;
    let mut thread_ids = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| unlisted(error.to_string()))?;
        let name = entry.file_name();
        let thread_id = name
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .ok_or_else(|| unlisted(format!("the entry {name:?} is not a thread id")))?;
        let _new = thread_ids.insert(thread_id);
        if thread_ids.len() > MAX_WORKER_THREADS {
            return Err(WorkerNotPlaced::TooManyThreads);
        }
    }
    let mut placed = 0;
    for thread_id in thread_ids {
        let Ok(target) = libc::pid_t::try_from(thread_id) else {
            // No thread has an id above the largest pid.
            continue;
        };
        match os::set_thread_cpus(target, cpus) {
            ThreadPlaced::Placed => placed += 1,
            ThreadPlaced::Gone => {}
            ThreadPlaced::Refused(cause) => {
                return Err(WorkerNotPlaced::ThreadRefused { thread_id, cause })
            }
        }
    }
    Ok(placed)
}

/// What one placement of a thread did.
enum ThreadPlaced {
    Placed,
    /// The thread ended before the call.
    Gone,
    Refused(String),
}

/// The CPUs that this thread may use.
pub fn allowed_cpus() -> Result<CpuIds, PlacementRefused> {
    os::allowed_cpus()
}

#[cfg(target_os = "linux")]
mod os {
    use std::io;

    use super::{CpuIds, PlacementRefused, ThreadPlaced, MAX_CPU_ID};

    const WORD_BITS: u32 = libc::c_ulong::BITS;

    /// The mask words of `cpus`, as the kernel reads a CPU mask.
    fn mask_of(cpus: &CpuIds) -> Vec<libc::c_ulong> {
        let highest = *cpus.ids().last().expect("a set of CPU ids is not empty");
        let words = usize::try_from(highest / WORD_BITS + 1).expect("at most 1024 words");
        let mut mask: Vec<libc::c_ulong> = vec![0; words];
        let one: libc::c_ulong = 1;
        for cpu in cpus.ids() {
            let word = usize::try_from(cpu / WORD_BITS).expect("at most 1024 words");
            mask[word] |= one << (cpu % WORD_BITS);
        }
        mask
    }

    fn set(target: libc::pid_t, cpus: &CpuIds) -> Result<(), io::Error> {
        let mask = mask_of(cpus);
        // SAFETY: the mask is a live buffer of the given size; the kernel
        // reads at most that many bytes of it.
        let set = unsafe {
            libc::sched_setaffinity(
                target,
                std::mem::size_of_val(mask.as_slice()),
                mask.as_ptr().cast::<libc::cpu_set_t>(),
            )
        };
        match set {
            0 => Ok(()),
            _ => Err(io::Error::last_os_error()),
        }
    }

    pub(super) fn set_cpus(target: libc::pid_t, cpus: &CpuIds) -> Result<(), String> {
        set(target, cpus).map_err(|error| error.to_string())
    }

    pub(super) fn set_thread_cpus(target: libc::pid_t, cpus: &CpuIds) -> ThreadPlaced {
        match set(target, cpus) {
            Ok(()) => ThreadPlaced::Placed,
            Err(error) => match error.raw_os_error() {
                Some(libc::ESRCH) => ThreadPlaced::Gone,
                _ => ThreadPlaced::Refused(error.to_string()),
            },
        }
    }

    pub(super) fn allowed_cpus() -> Result<CpuIds, PlacementRefused> {
        let words = usize::try_from((MAX_CPU_ID + 1) / WORD_BITS).expect("at most 1024 words");
        let mut mask: Vec<libc::c_ulong> = vec![0; words];
        // SAFETY: the mask is a live, writable buffer of the given size.
        let read = unsafe {
            libc::sched_getaffinity(
                0,
                std::mem::size_of_val(mask.as_slice()),
                mask.as_mut_ptr().cast::<libc::cpu_set_t>(),
            )
        };
        if read != 0 {
            return Err(PlacementRefused::AllowedUnreadable {
                cause: io::Error::last_os_error().to_string(),
            });
        }
        let mut ids = std::collections::BTreeSet::new();
        for (index, word) in mask.iter().enumerate() {
            let base = u32::try_from(index).expect("at most 1024 words") * WORD_BITS;
            for bit in 0..WORD_BITS {
                if (*word >> bit) & 1 == 1 {
                    let _new = ids.insert(base + bit);
                }
            }
        }
        CpuIds::new(ids).ok_or_else(|| PlacementRefused::AllowedUnreadable {
            cause: "the CPU mask of this thread is empty".to_owned(),
        })
    }
}

#[cfg(not(target_os = "linux"))]
mod os {
    use super::{CpuIds, PlacementRefused, ThreadPlaced};

    const NOT_LINUX: &str = "setting the CPUs of a thread or a process needs Linux";

    pub(super) fn set_cpus(_target: libc::pid_t, _cpus: &CpuIds) -> Result<(), String> {
        Err(NOT_LINUX.to_owned())
    }

    pub(super) fn set_thread_cpus(_target: libc::pid_t, _cpus: &CpuIds) -> ThreadPlaced {
        ThreadPlaced::Refused(NOT_LINUX.to_owned())
    }

    pub(super) fn allowed_cpus() -> Result<CpuIds, PlacementRefused> {
        Err(PlacementRefused::NotLinux {
            platform: std::env::consts::OS,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cpus(ids: &[u32]) -> CpuIds {
        CpuIds::new(ids.iter().copied().collect()).unwrap()
    }

    #[test]
    fn a_cpu_list_reads_ranges_and_single_cpus() {
        assert_eq!(
            parse_cpu_list("0-3,8-11\n"),
            Ok(cpus(&[0, 1, 2, 3, 8, 9, 10, 11]))
        );
        assert_eq!(parse_cpu_list("6,7"), Ok(cpus(&[6, 7])));
        assert_eq!(parse_cpu_list("5"), Ok(cpus(&[5])));
    }

    /// The refusal names the text.
    #[test]
    fn the_refusal_of_a_cpu_list_names_its_text() {
        for text in ["", "a", "3-1", "1-", "-2", "70000", "1,,2"] {
            let refused = parse_cpu_list(text).expect_err(text);
            assert_eq!(refused.text, text);
            assert!(
                refused.to_string().contains(&format!("{text:?}")),
                "{refused}"
            );
        }
    }

    #[test]
    fn a_text_that_is_not_a_cpu_list_is_refused() {
        for text in ["", "a", "3-1", "1-", "-1", "1,,2", "65536", "0-65536", "+1"] {
            assert!(parse_cpu_list(text).is_err(), "{text:?}");
        }
    }

    fn smt_siblings() -> BTreeMap<u32, CpuIds> {
        BTreeMap::from([
            (0, cpus(&[0, 2])),
            (1, cpus(&[1, 3])),
            (2, cpus(&[0, 2])),
            (3, cpus(&[1, 3])),
        ])
    }

    #[test]
    fn the_worker_takes_the_core_of_the_lowest_cpu() {
        let split = split_own_core(&cpus(&[0, 1, 2, 3]), &smt_siblings()).unwrap();

        assert_eq!(split.parent(), &cpus(&[0, 2]));
        assert_eq!(split.children(), &cpus(&[1, 3]));
    }

    #[test]
    fn a_core_is_cut_to_the_allowed_cpus() {
        let split = split_own_core(&cpus(&[2, 3]), &smt_siblings()).unwrap();

        assert_eq!(split.parent(), &cpus(&[2]));
        assert_eq!(split.children(), &cpus(&[3]));
    }

    #[test]
    fn one_physical_core_is_too_few() {
        let refused = split_own_core(&cpus(&[0, 2]), &smt_siblings()).unwrap_err();

        assert_eq!(refused.cores, 1);
        assert_eq!(refused.allowed, cpus(&[0, 2]));
        assert!(refused.to_string().contains("at least 2 physical cores"));
    }

    #[test]
    fn cpus_without_smt_siblings_are_one_core_each() {
        let alone: BTreeMap<u32, CpuIds> = (0..3).map(|cpu| (cpu, cpus(&[cpu]))).collect();

        let split = split_own_core(&cpus(&[0, 1, 2]), &alone).unwrap();

        assert_eq!(split.parent(), &cpus(&[0]));
        assert_eq!(split.children(), &cpus(&[1, 2]));
    }

    #[test]
    fn the_placement_gives_the_children_their_cpus() {
        let split = split_own_core(&cpus(&[0, 1, 2, 3]), &smt_siblings()).unwrap();

        assert_eq!(CpuPlacement::SharedCpus.child_cpus(), ChildCpus::Inherited);
        assert_eq!(
            CpuPlacement::OwnCore(split).child_cpus(),
            ChildCpus::Set(cpus(&[1, 3]))
        );
    }

    /// A topology directory with one sibling list per CPU.
    fn topology(lists: &[(u32, &str)]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "horsies-topology-{}",
            uuid::Uuid::new_v4().simple()
        ));
        for (cpu, list) in lists {
            let topology = dir.join(format!("cpu{cpu}")).join("topology");
            std::fs::create_dir_all(&topology).unwrap();
            std::fs::write(topology.join("thread_siblings_list"), list).unwrap();
        }
        dir
    }

    #[test]
    fn the_siblings_are_read_from_the_topology() {
        let dir = topology(&[(0, "0,2\n"), (2, "0,2\n")]);

        let siblings = read_siblings(&cpus(&[0, 2]), &dir);

        assert_eq!(
            siblings,
            Ok(BTreeMap::from([(0, cpus(&[0, 2])), (2, cpus(&[0, 2]))]))
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_missing_or_wrong_sibling_list_is_unreadable() {
        let dir = topology(&[(0, "1\n"), (1, "garbage")]);

        let omits = read_siblings(&cpus(&[0]), &dir).unwrap_err();
        let garbage = read_siblings(&cpus(&[1]), &dir).unwrap_err();
        let missing = read_siblings(&cpus(&[7]), &dir).unwrap_err();

        assert_eq!(omits.cpu, 0);
        assert!(omits.cause.contains("omits it"), "{}", omits.cause);
        assert_eq!(garbage.cpu, 1);
        assert_eq!(missing.cpu, 7);
        assert!(
            missing.cause.contains("cpu7/topology/thread_siblings_list"),
            "{}",
            missing.cause
        );
        assert!(
            missing.cause.contains("No such file or directory"),
            "{}",
            missing.cause
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn each_refusal_names_its_cause() {
        let refusals = [
            (
                PlacementRefused::NotLinux { platform: "macos" },
                "Linux only",
            ),
            (
                PlacementRefused::TooFewCores(TooFewCores {
                    allowed: cpus(&[6, 7]),
                    cores: 1,
                }),
                "at least 2 physical cores",
            ),
            (
                PlacementRefused::TopologyUnreadable(TopologyUnreadable {
                    cpu: 3,
                    cause: "gone".to_owned(),
                }),
                "CPU 3",
            ),
        ];
        for (refused, phrase) in refusals {
            assert!(refused.to_string().contains(phrase), "{refused}");
        }
    }

    /// On Linux the plan is the split of the CPUs that this process may use
    /// by the SMT siblings that the host topology lists.
    #[cfg(target_os = "linux")]
    #[test]
    fn own_core_on_linux_splits_the_allowed_cpus() {
        let allowed = allowed_cpus().unwrap();
        let expected = read_siblings(&allowed, Path::new(CPU_TOPOLOGY_DIR))
            .map_err(PlacementRefused::TopologyUnreadable)
            .and_then(|siblings| {
                split_own_core(&allowed, &siblings).map_err(PlacementRefused::TooFewCores)
            });

        assert_eq!(plan_own_core(), expected);
    }

    /// Python `test_a_refused_placement_stops_the_worker_start`, the plan:
    /// off Linux the plan is refused with a message that names Linux.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn own_core_is_refused_off_linux() {
        let refused = plan_own_core().unwrap_err();

        assert!(matches!(refused, PlacementRefused::NotLinux { .. }));
        assert!(refused.to_string().contains("Linux only"), "{refused}");
    }

    /// A thread that a test owns: its id, and the sender that ends it.
    #[cfg(target_os = "linux")]
    struct OwnThread {
        thread_id: u32,
        end: std::sync::mpsc::Sender<()>,
        handle: std::thread::JoinHandle<()>,
    }

    #[cfg(target_os = "linux")]
    impl OwnThread {
        fn start() -> Self {
            let (id_tx, id_rx) = std::sync::mpsc::channel();
            let (end, end_rx) = std::sync::mpsc::channel::<()>();
            let handle = std::thread::spawn(move || {
                // SAFETY: gettid takes no argument.
                let thread_id = unsafe { libc::syscall(libc::SYS_gettid) };
                id_tx.send(u32::try_from(thread_id).unwrap()).unwrap();
                let _ = end_rx.recv();
            });
            Self {
                thread_id: id_rx.recv().unwrap(),
                end,
                handle,
            }
        }

        /// A listing that names only this thread.
        fn listing(&self) -> (WorkerThreads, PathBuf) {
            let dir = std::env::temp_dir()
                .join(format!("horsies-threads-{}", uuid::Uuid::new_v4().simple()));
            std::fs::create_dir_all(dir.join(self.thread_id.to_string())).unwrap();
            (WorkerThreads::listed_in(dir.clone()), dir)
        }

        fn cpus(&self) -> CpuIds {
            allowed_list(&format!("/proc/self/task/{}/status", self.thread_id))
        }

        fn end(self) {
            self.end.send(()).unwrap();
            self.handle.join().unwrap();
        }
    }

    /// The `Cpus_allowed_list` of a status file.
    #[cfg(target_os = "linux")]
    fn allowed_list(status_path: &str) -> CpuIds {
        let status = std::fs::read_to_string(status_path).unwrap();
        let line = status
            .lines()
            .find(|line| line.starts_with("Cpus_allowed_list:"))
            .expect("the status names the allowed CPUs");
        parse_cpu_list(line.split_once(':').unwrap().1).unwrap()
    }

    /// Python `test_the_worker_places_its_threads_on_its_core`: every listed
    /// thread gets the CPUs.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_worker_places_its_threads_on_its_core() {
        let thread = OwnThread::start();
        let (threads, dir) = thread.listing();
        let lowest = cpus(&[allowed_cpus().unwrap().lowest()]);

        let placed = place_worker_threads(&lowest, &threads);

        assert_eq!(placed, Ok(1));
        assert_eq!(thread.cpus(), lowest);
        thread.end();
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Python `test_a_refused_worker_thread_stops_the_start`: the refusal
    /// names the thread.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_refused_worker_thread_is_named() {
        let thread = OwnThread::start();
        let (threads, dir) = thread.listing();
        let before = thread.cpus();

        let refused = place_worker_threads(&cpus(&[MAX_CPU_ID]), &threads);

        match refused {
            Err(WorkerNotPlaced::ThreadRefused { thread_id, .. }) => {
                assert_eq!(thread_id, thread.thread_id);
            }
            other => panic!("expected a refused thread, got {other:?}"),
        }
        assert!(refused_text_names_the_thread(thread.thread_id));
        assert_eq!(thread.cpus(), before);
        thread.end();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(target_os = "linux")]
    fn refused_text_names_the_thread(thread_id: u32) -> bool {
        WorkerNotPlaced::ThreadRefused {
            thread_id,
            cause: "Invalid argument (os error 22)".to_owned(),
        }
        .to_string()
        .contains(&format!("worker thread {thread_id}"))
    }

    /// Every thread of the listing gets the CPUs, and the count is the count
    /// of the listing.
    #[cfg(target_os = "linux")]
    #[test]
    fn every_listed_thread_gets_the_cpus() {
        let threads: Vec<OwnThread> = (0..3).map(|_| OwnThread::start()).collect();
        let dir =
            std::env::temp_dir().join(format!("horsies-threads-{}", uuid::Uuid::new_v4().simple()));
        for thread in &threads {
            std::fs::create_dir_all(dir.join(thread.thread_id.to_string())).unwrap();
        }
        let lowest = cpus(&[allowed_cpus().unwrap().lowest()]);

        let placed = place_worker_threads(&lowest, &WorkerThreads::listed_in(dir.clone()));

        assert_eq!(placed, Ok(3));
        for thread in &threads {
            assert_eq!(thread.cpus(), lowest);
        }
        for thread in threads {
            thread.end();
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A thread that ended between the listing and the call is skipped.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_thread_that_ended_is_skipped() {
        let dir =
            std::env::temp_dir().join(format!("horsies-threads-{}", uuid::Uuid::new_v4().simple()));
        // No thread has an id above the largest pid.
        std::fs::create_dir_all(dir.join(i32::MAX.to_string())).unwrap();
        let lowest = cpus(&[allowed_cpus().unwrap().lowest()]);

        let placed = place_worker_threads(&lowest, &WorkerThreads::listed_in(dir.clone()));

        assert_eq!(placed, Ok(0));
        let _ = std::fs::remove_dir_all(dir);
    }
}
