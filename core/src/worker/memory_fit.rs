//! Whether the per-child memory limits of a worker profile fit the memory
//! that the worker can use.
//!
//! - The sum of `recycle.above` over the isolated, shared async and shared
//!   thread children is compared with the usable memory. A mode with
//!   children and no memory limit gives no sum. A sum above the usable
//!   memory exceeds it; an equal sum fits.
//! - Usable memory: the cgroup limit of this process when it is below the
//!   host RAM, else the host RAM. An unreadable cgroup falls back to the
//!   host RAM; nothing readable is unreadable.
//! - At boot the worker logs the result and starts in every case: a
//!   deployment can move between machine sizes.
//!
//! The readers do bounded file I/O and return a closed outcome; the
//! comparisons and the log records are pure.

use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use crate::core::config::worker_profile::{
    quoted_profile_name, IsolatedSetting, MemoryLimitSetting, SharedAsyncSetting,
    SharedThreadSetting, WorkerProfile,
};
use crate::worker::connection_fit::BootLogLevel;

#[cfg(test)]
mod tests;

/// The cgroups of this process, one line per hierarchy:
/// `<id>:<controllers>:<path>`. The cgroup v2 line is `0::<path>`.
pub const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

/// The mount point of the cgroup v2 hierarchy.
pub const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// The mount point of the cgroup v1 memory controller.
pub const CGROUP_V1_MEMORY_ROOT: &str = "/sys/fs/cgroup/memory";

pub const CGROUP_V2_LIMIT_FILE: &str = "memory.max";
pub const CGROUP_V1_LIMIT_FILE: &str = "memory.limit_in_bytes";

/// The most bytes read from a cgroup limit file. A limit is one decimal
/// number of at most 20 digits, or `max`.
pub const CGROUP_FILE_MAX_BYTES: usize = 64;

/// The most bytes read from `/proc/self/cgroup`: about 15 lines, each with a
/// path of a few hundred bytes at most.
pub const PROC_CGROUP_MAX_BYTES: usize = 64 * 1024;

/// The most path parts of a cgroup that the reader walks. A deeper path is
/// not resolved, and the reader falls back to the limit files at the mount
/// points.
pub const CGROUP_PATH_MAX_DEPTH: usize = 32;

const CGROUP_NO_LIMIT: &str = "max";

const BYTES_PER_MB: u64 = 1024 * 1024;

/// The total RAM of the host, in MB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MachineMemory {
    pub mb: u64,
}

/// The RAM of the host could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineMemoryUnreadable {
    pub reason: String,
}

/// Read the total RAM of the host. Inside a container this is the RAM of the
/// host, not a cgroup limit ([`read_cgroup_memory_limit`]).
pub fn read_machine_memory() -> Result<MachineMemory, MachineMemoryUnreadable> {
    let mut system = sysinfo::System::new();
    system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::new().with_ram());
    machine_memory_of(system.total_memory())
}

/// The RAM of `total_bytes` bytes in whole MB. The system reader reports 0
/// when it could not read the RAM size. Pure.
pub fn machine_memory_of(total_bytes: u64) -> Result<MachineMemory, MachineMemoryUnreadable> {
    match total_bytes {
        0 => Err(MachineMemoryUnreadable {
            reason: "the host reported no RAM size".to_owned(),
        }),
        bytes => Ok(MachineMemory {
            mb: bytes / BYTES_PER_MB,
        }),
    }
}

/// The memory limit of the cgroup of this process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CgroupMemoryLimit {
    /// The cgroup limits the memory of the process to `limit_bytes`.
    Limit { limit_bytes: u64 },
    /// No cgroup limit applies: the limit file says `max`, or no limit file
    /// exists (no memory controller, or not Linux).
    NoLimit,
    /// A limit file exists and could not be read or parsed.
    Unreadable { reason: String },
}

/// Where the reader finds the cgroup files. Tests point it at files of their
/// own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupRoots {
    pub proc_self_cgroup: PathBuf,
    pub v2_root: PathBuf,
    pub v1_memory_root: PathBuf,
}

impl CgroupRoots {
    /// The files of this process on Linux.
    pub fn of_this_process() -> CgroupRoots {
        CgroupRoots {
            proc_self_cgroup: PathBuf::from(PROC_SELF_CGROUP),
            v2_root: PathBuf::from(CGROUP_V2_ROOT),
            v1_memory_root: PathBuf::from(CGROUP_V1_MEMORY_ROOT),
        }
    }
}

/// The group of this process in one hierarchy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CgroupPath {
    /// The process is in this group, given as path parts.
    InCgroup(Vec<String>),
    /// `/proc/self/cgroup` names no usable group for the hierarchy: no line,
    /// a path that leaves the mount point (`..`), or a path deeper than
    /// [`CGROUP_PATH_MAX_DEPTH`].
    NoCgroupPath,
}

/// The groups of this process that hold a memory limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcCgroup {
    pub v2: CgroupPath,
    pub v1_memory: CgroupPath,
}

fn group_of(path: &str) -> CgroupPath {
    let parts: Vec<String> = path
        .split('/')
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect();
    match parts.iter().any(|part| part == "..") || parts.len() > CGROUP_PATH_MAX_DEPTH {
        true => CgroupPath::NoCgroupPath,
        false => CgroupPath::InCgroup(parts),
    }
}

/// The cgroup v2 group and the cgroup v1 memory group of `text`. A line
/// that has not three fields is skipped. Pure.
pub fn parse_proc_cgroup(text: &str) -> ProcCgroup {
    let mut parsed = ProcCgroup {
        v2: CgroupPath::NoCgroupPath,
        v1_memory: CgroupPath::NoCgroupPath,
    };
    for line in text.lines() {
        let mut fields = line.splitn(3, ':');
        match (fields.next(), fields.next(), fields.next()) {
            (Some("0"), Some(""), Some(path)) => parsed.v2 = group_of(path),
            (Some(_), Some(controllers), Some(path))
                if controllers
                    .split(',')
                    .any(|controller| controller == "memory") =>
            {
                parsed.v1_memory = group_of(path);
            }
            _ => {}
        }
    }
    parsed
}

/// One bounded read of a file.
enum FileRead {
    Absent,
    Bytes(Vec<u8>),
    AboveBound,
    Failed(String),
}

/// Read at most `bound` bytes of `path`; one byte more says it is longer.
fn read_bounded(path: &Path, bound: usize) -> FileRead {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) => {
            return match error.kind() {
                ErrorKind::NotFound => FileRead::Absent,
                kind => FileRead::Failed(format!("{}: {kind:?}: {error}", path.display())),
            }
        }
    };
    let limit = u64::try_from(bound).unwrap_or(u64::MAX).saturating_add(1);
    let mut bytes = Vec::with_capacity(bound.saturating_add(1));
    match file.take(limit).read_to_end(&mut bytes) {
        Ok(_read) => {}
        Err(error) => {
            return FileRead::Failed(format!("{}: {:?}: {error}", path.display(), error.kind()))
        }
    }
    match bytes.len() > bound {
        true => FileRead::AboveBound,
        false => FileRead::Bytes(bytes),
    }
}

/// A limit file: absent, or what it says.
enum LimitFile {
    Absent,
    Says(CgroupMemoryLimit),
}

/// Read and parse one limit file: `max` or a decimal number of bytes.
fn limit_of(path: &Path) -> LimitFile {
    let bytes = match read_bounded(path, CGROUP_FILE_MAX_BYTES) {
        FileRead::Absent => return LimitFile::Absent,
        FileRead::Bytes(bytes) => bytes,
        FileRead::AboveBound => {
            return LimitFile::Says(CgroupMemoryLimit::Unreadable {
                reason: format!(
                    "{}: more than {CGROUP_FILE_MAX_BYTES} bytes",
                    path.display()
                ),
            })
        }
        FileRead::Failed(reason) => {
            return LimitFile::Says(CgroupMemoryLimit::Unreadable { reason })
        }
    };
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) if text.is_ascii() => text.trim(),
        Ok(_) | Err(_) => {
            return LimitFile::Says(CgroupMemoryLimit::Unreadable {
                reason: format!("{}: not ASCII text", path.display()),
            })
        }
    };
    LimitFile::Says(parse_limit_value(path, text))
}

fn parse_limit_value(path: &Path, value: &str) -> CgroupMemoryLimit {
    let digits = !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit());
    match (value == CGROUP_NO_LIMIT, digits) {
        (true, _) => CgroupMemoryLimit::NoLimit,
        (false, true) => match value.parse::<u64>() {
            Ok(limit_bytes) => CgroupMemoryLimit::Limit { limit_bytes },
            Err(_too_large) => CgroupMemoryLimit::Unreadable {
                reason: format!(
                    "{}: the limit {value} is above the largest count of bytes",
                    path.display()
                ),
            },
        },
        (false, false) => CgroupMemoryLimit::Unreadable {
            reason: format!("{}: unexpected content '{value}'", path.display()),
        },
    }
}

/// The limit file of the group, then of each parent group up to `root`.
fn limit_files(root: &Path, group: &CgroupPath, file_name: &str) -> Vec<PathBuf> {
    match group {
        CgroupPath::InCgroup(parts) => (0..=parts.len())
            .rev()
            .map(|depth| {
                let mut path = root.to_path_buf();
                for part in &parts[..depth] {
                    path.push(part);
                }
                path.push(file_name);
                path
            })
            .collect(),
        CgroupPath::NoCgroupPath => vec![root.join(file_name)],
    }
}

/// The smallest limit of a chain of files, so far.
#[derive(Clone, Copy)]
enum Smallest {
    NoFile,
    NoLimit,
    Limit(u64),
}

/// The smallest limit of `files`: a group is limited by each parent. `max`
/// is no limit at that level. `None` when no file exists. The first
/// unreadable file is the outcome.
fn smallest_limit(files: &[PathBuf]) -> Option<CgroupMemoryLimit> {
    let mut smallest = Smallest::NoFile;
    for path in files {
        match (limit_of(path), smallest) {
            (LimitFile::Says(unreadable @ CgroupMemoryLimit::Unreadable { .. }), _) => {
                return Some(unreadable)
            }
            (LimitFile::Absent, _)
            | (
                LimitFile::Says(CgroupMemoryLimit::NoLimit),
                Smallest::NoLimit | Smallest::Limit(_),
            ) => {}
            (LimitFile::Says(CgroupMemoryLimit::NoLimit), Smallest::NoFile) => {
                smallest = Smallest::NoLimit;
            }
            (LimitFile::Says(CgroupMemoryLimit::Limit { limit_bytes }), Smallest::Limit(held))
                if held <= limit_bytes => {}
            (LimitFile::Says(CgroupMemoryLimit::Limit { limit_bytes }), _) => {
                smallest = Smallest::Limit(limit_bytes);
            }
        }
    }
    match smallest {
        Smallest::NoFile => None,
        Smallest::NoLimit => Some(CgroupMemoryLimit::NoLimit),
        Smallest::Limit(limit_bytes) => Some(CgroupMemoryLimit::Limit { limit_bytes }),
    }
}

/// Read `/proc/self/cgroup`. Absent, unreadable or longer than its bound:
/// not read.
fn read_proc_cgroup(path: &Path) -> Option<ProcCgroup> {
    match read_bounded(path, PROC_CGROUP_MAX_BYTES) {
        FileRead::Bytes(bytes) => Some(parse_proc_cgroup(&String::from_utf8_lossy(&bytes))),
        FileRead::Absent | FileRead::AboveBound | FileRead::Failed(_) => None,
    }
}

/// Read the memory limit of the cgroup of this process.
///
/// `/proc/self/cgroup` names the group of the process: the cgroup v1 memory
/// group when one is named, else the cgroup v2 group. The limit is the
/// smallest of the group and its parent groups, up to the mount point (a
/// systemd `MemoryMax` of a service or a slice, or a container seen with
/// the host cgroup namespace). Without a group, the reader takes the cgroup
/// v2 file at the mount point, else the cgroup v1 file there. No file means
/// no cgroup limit. The cgroup v1 value for "no limit" is a number far above
/// any RAM; [`usable_memory`] then keeps the host RAM.
pub fn read_cgroup_memory_limit(roots: &CgroupRoots) -> CgroupMemoryLimit {
    let chains: Vec<Vec<PathBuf>> = match read_proc_cgroup(&roots.proc_self_cgroup) {
        Some(ProcCgroup {
            v1_memory: group @ CgroupPath::InCgroup(_),
            ..
        }) => vec![limit_files(
            &roots.v1_memory_root,
            &group,
            CGROUP_V1_LIMIT_FILE,
        )],
        Some(ProcCgroup {
            v2: group @ CgroupPath::InCgroup(_),
            ..
        }) => vec![limit_files(&roots.v2_root, &group, CGROUP_V2_LIMIT_FILE)],
        Some(ProcCgroup { .. }) | None => vec![
            vec![roots.v2_root.join(CGROUP_V2_LIMIT_FILE)],
            vec![roots.v1_memory_root.join(CGROUP_V1_LIMIT_FILE)],
        ],
    };
    for files in &chains {
        if let Some(outcome) = smallest_limit(files) {
            return outcome;
        }
    }
    CgroupMemoryLimit::NoLimit
}

/// Where the memory that a worker can use comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySource {
    ContainerLimit,
    HostRam,
}

impl MemorySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContainerLimit => "container limit",
            Self::HostRam => "host RAM",
        }
    }
}

/// The memory that the worker can use, in MB, and where it comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsableMemory {
    pub mb: u64,
    pub source: MemorySource,
}

/// Neither a cgroup limit nor the host RAM could be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsableMemoryUnreadable {
    pub reason: String,
}

/// The smaller of the cgroup limit and the host RAM. A cgroup limit at or
/// above the host RAM (the cgroup v1 value for "no limit" included) leaves
/// the host RAM. An unreadable cgroup leaves the host RAM; the caller
/// reports it. Pure.
pub fn usable_memory(
    host: &Result<MachineMemory, MachineMemoryUnreadable>,
    cgroup: &CgroupMemoryLimit,
) -> Result<UsableMemory, UsableMemoryUnreadable> {
    match (cgroup, host) {
        (CgroupMemoryLimit::Limit { limit_bytes }, Ok(host)) => {
            let container_mb = limit_bytes / BYTES_PER_MB;
            match container_mb < host.mb {
                true => Ok(UsableMemory {
                    mb: container_mb,
                    source: MemorySource::ContainerLimit,
                }),
                false => Ok(UsableMemory {
                    mb: host.mb,
                    source: MemorySource::HostRam,
                }),
            }
        }
        (CgroupMemoryLimit::Limit { limit_bytes }, Err(_unreadable)) => Ok(UsableMemory {
            mb: limit_bytes / BYTES_PER_MB,
            source: MemorySource::ContainerLimit,
        }),
        (CgroupMemoryLimit::NoLimit | CgroupMemoryLimit::Unreadable { .. }, Ok(host)) => {
            Ok(UsableMemory {
                mb: host.mb,
                source: MemorySource::HostRam,
            })
        }
        (CgroupMemoryLimit::NoLimit | CgroupMemoryLimit::Unreadable { .. }, Err(unreadable)) => {
            Err(UsableMemoryUnreadable {
                reason: unreadable.reason.clone(),
            })
        }
    }
}

/// The sum of the per-child memory limits against the usable memory
/// (`machine_mb`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryLimitsFit {
    /// The sum is at or below the usable memory.
    Fit { total_mb: u64, machine_mb: u64 },
    /// The sum is above the usable memory.
    ExceedMachine { total_mb: u64, machine_mb: u64 },
    /// A mode of the profile has children with no memory limit, so no sum
    /// exists.
    NotSet,
}

/// The memory limits of the children of one mode.
enum ModeMemory {
    Mb(u64),
    NotSet,
}

fn mode_memory(count: u32, limit: &MemoryLimitSetting) -> ModeMemory {
    match limit {
        MemoryLimitSetting::MemoryLimit { mb } => {
            ModeMemory::Mb(u64::from(count) * u64::from(mb.get()))
        }
        MemoryLimitSetting::NoMemoryLimit {} => ModeMemory::NotSet,
    }
}

fn isolated_memory(isolated: &IsolatedSetting) -> ModeMemory {
    match isolated {
        IsolatedSetting::IsolatedChildren(children) => {
            mode_memory(children.count.get(), &children.recycle.above)
        }
        IsolatedSetting::NoIsolatedChildren {} => ModeMemory::Mb(0),
    }
}

fn shared_async_memory(shared_async: &SharedAsyncSetting) -> ModeMemory {
    match shared_async {
        SharedAsyncSetting::SharedAsyncChildren(children) => {
            mode_memory(children.count.get(), &children.recycle.above)
        }
        SharedAsyncSetting::NoSharedAsyncChildren {} => ModeMemory::Mb(0),
    }
}

fn shared_thread_memory(shared_thread: &SharedThreadSetting) -> ModeMemory {
    match shared_thread {
        SharedThreadSetting::SharedThreadChildren(children) => {
            mode_memory(children.count.get(), &children.recycle.above)
        }
        SharedThreadSetting::NoSharedThreadChildren {} => ModeMemory::Mb(0),
    }
}

/// Compare the sum over every mode of the per-child memory limits with the
/// usable memory. Pure.
pub fn compare_memory_limits(profile: &WorkerProfile, machine: &UsableMemory) -> MemoryLimitsFit {
    let mut total_mb: u64 = 0;
    for mode in [
        isolated_memory(&profile.isolated),
        shared_async_memory(&profile.shared_async),
        shared_thread_memory(&profile.shared_thread),
    ] {
        match mode {
            ModeMemory::NotSet => return MemoryLimitsFit::NotSet,
            ModeMemory::Mb(mb) => total_mb = total_mb.saturating_add(mb),
        }
    }
    match total_mb > machine.mb {
        true => MemoryLimitsFit::ExceedMachine {
            total_mb,
            machine_mb: machine.mb,
        },
        false => MemoryLimitsFit::Fit {
            total_mb,
            machine_mb: machine.mb,
        },
    }
}

/// The memory fit of the profile that a worker runs, read at boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootMemoryFit {
    /// The cgroup reading. An unreadable one gets its own warning: the host
    /// RAM is then the only bound, and inside a container it can be far too
    /// high.
    pub cgroup: CgroupMemoryLimit,
    pub comparison: MemoryComparison,
}

/// The comparison of the boot memory fit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryComparison {
    /// Neither the cgroup limit nor the host RAM was read: nothing compared.
    MemoryUnreadable(UsableMemoryUnreadable),
    /// The sum was compared with the usable memory of `source`.
    Compared {
        source: MemorySource,
        fit: MemoryLimitsFit,
    },
}

/// The memory fit of `profile` from the two readings. Pure.
pub fn boot_memory_fit(
    profile: &WorkerProfile,
    host: &Result<MachineMemory, MachineMemoryUnreadable>,
    cgroup: CgroupMemoryLimit,
) -> BootMemoryFit {
    let comparison = match usable_memory(host, &cgroup) {
        Ok(usable) => MemoryComparison::Compared {
            source: usable.source,
            fit: compare_memory_limits(profile, &usable),
        },
        Err(unreadable) => MemoryComparison::MemoryUnreadable(unreadable),
    };
    BootMemoryFit { cgroup, comparison }
}

/// Reads the host RAM and the cgroup limit of this process, and compares
/// `profile` with them. Reads two or three small files; writes nothing;
/// never refuses.
pub fn read_boot_memory_fit(profile: &WorkerProfile) -> BootMemoryFit {
    let cgroup = read_cgroup_memory_limit(&CgroupRoots::of_this_process());
    boot_memory_fit(profile, &read_machine_memory(), cgroup)
}

/// One log record of the boot memory fit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootMemoryLog {
    pub level: BootLogLevel,
    pub message: String,
}

/// The records that the worker logs for the memory fit of its profile
/// `name`, with Python's levels and texts (`clients/python/horsies/core/cli.py:617-688`):
/// an unreadable cgroup is a warning; then the comparison: a sum above the
/// usable memory or an unreadable memory is a warning, a fitting sum and no
/// sum are info. Pure.
pub fn boot_memory_log(name: &str, fit: &BootMemoryFit) -> Vec<BootMemoryLog> {
    let profile = quoted_profile_name(name);
    let mut records = Vec::with_capacity(2);
    match &fit.cgroup {
        CgroupMemoryLimit::Unreadable { reason } => records.push(BootMemoryLog {
            level: BootLogLevel::Warning,
            message: format!(
                "Worker profile {profile}: the container memory limit could not be read \
                 ({reason}); the per-child memory limits are compared with the host RAM"
            ),
        }),
        CgroupMemoryLimit::Limit { .. } | CgroupMemoryLimit::NoLimit => {}
    }
    records.push(match &fit.comparison {
        MemoryComparison::MemoryUnreadable(unreadable) => BootMemoryLog {
            level: BootLogLevel::Warning,
            message: format!(
                "Worker profile {profile}: the memory of the machine could not be read ({}); \
                 the per-child memory limits were not compared with it",
                unreadable.reason
            ),
        },
        MemoryComparison::Compared {
            source,
            fit:
                MemoryLimitsFit::ExceedMachine {
                    total_mb,
                    machine_mb,
                },
        } => BootMemoryLog {
            level: BootLogLevel::Warning,
            message: format!(
                "Worker profile {profile}: the per-child memory limits add up to {total_mb} MB, \
                 above the {machine_mb} MB {}. Children can use more memory than the worker \
                 has before they are recycled.",
                source.as_str()
            ),
        },
        MemoryComparison::Compared {
            source,
            fit:
                MemoryLimitsFit::Fit {
                    total_mb,
                    machine_mb,
                },
        } => BootMemoryLog {
            level: BootLogLevel::Info,
            message: format!(
                "Worker profile {profile}: per-child memory limits add up to {total_mb} MB of \
                 the {machine_mb} MB {}",
                source.as_str()
            ),
        },
        MemoryComparison::Compared {
            fit: MemoryLimitsFit::NotSet,
            ..
        } => BootMemoryLog {
            level: BootLogLevel::Info,
            message: format!(
                "Worker profile {profile}: no per-child memory limit, so there is no sum to \
                 compare with the memory"
            ),
        },
    });
    records
}

/// Writes each record to the log at its level.
pub fn log_boot_memory_fit(records: &[BootMemoryLog]) {
    for record in records {
        match record.level {
            BootLogLevel::Info => tracing::info!("{}", record.message),
            BootLogLevel::Warning => tracing::warn!("{}", record.message),
        }
    }
}
