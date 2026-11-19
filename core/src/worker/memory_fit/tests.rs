//! The memory fit of a worker profile (Python
//! `clients/python/tests/unit/test_worker_profile_fit.py`, the memory classes, and
//! `clients/python/tests/unit/test_cli.py::TestBootMemoryFit`). Each test keeps the name of
//! its Python test.

use std::fs;

use uuid::Uuid;

use super::*;
use crate::core::config::bounded::BoundedU32;
use crate::core::config::worker_profile::{
    IsolatedChildren, IsolatedRecycle, SharedAsyncChildren, SharedRecycle, SharedThreadChildren,
};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// The cgroup v1 value for "no limit" on 64-bit Linux with 4 KiB pages.
const V1_NO_LIMIT: u64 = 9_223_372_036_854_771_712;

fn bounded<const MIN: u32, const MAX: u32>(value: u32) -> BoundedU32<MIN, MAX> {
    BoundedU32::try_from(value).expect("a test value inside its bounds")
}

fn memory_limit(mb: u32) -> MemoryLimitSetting {
    MemoryLimitSetting::MemoryLimit { mb: bounded(mb) }
}

fn isolated_limited(count: u32, mb: u32) -> IsolatedSetting {
    IsolatedSetting::IsolatedChildren(IsolatedChildren {
        recycle: IsolatedRecycle {
            above: memory_limit(mb),
            ..IsolatedRecycle::default()
        },
        ..IsolatedChildren::new(bounded(count))
    })
}

fn isolated_unlimited(count: u32) -> IsolatedSetting {
    IsolatedSetting::IsolatedChildren(IsolatedChildren::new(bounded(count)))
}

fn shared_async(count: u32, seats: u32, above: MemoryLimitSetting) -> SharedAsyncSetting {
    SharedAsyncSetting::SharedAsyncChildren(SharedAsyncChildren {
        recycle: SharedRecycle {
            above,
            ..SharedRecycle::default()
        },
        ..SharedAsyncChildren::new(bounded(count), bounded(seats))
    })
}

fn shared_thread(count: u32, seats: u32, above: MemoryLimitSetting) -> SharedThreadSetting {
    SharedThreadSetting::SharedThreadChildren(SharedThreadChildren {
        recycle: SharedRecycle {
            above,
            ..SharedRecycle::default()
        },
        ..SharedThreadChildren::new(bounded(count), bounded(seats))
    })
}

/// Python `_limited`: `count` isolated children of `mb` MB each.
fn limited(count: u32, mb: u32) -> WorkerProfile {
    WorkerProfile {
        isolated: isolated_limited(count, mb),
        ..WorkerProfile::default()
    }
}

fn host_ram(mb: u64) -> UsableMemory {
    UsableMemory {
        mb,
        source: MemorySource::HostRam,
    }
}

// ---------------------------------------------------------------------------
// TestCompareMemoryLimits
// ---------------------------------------------------------------------------

#[test]
fn sum_above_the_usable_memory_exceeds() {
    assert_eq!(
        compare_memory_limits(&limited(4, 512), &host_ram(2047)),
        MemoryLimitsFit::ExceedMachine {
            total_mb: 2048,
            machine_mb: 2047
        }
    );
}

#[test]
fn sum_equal_to_the_usable_memory_fits() {
    assert_eq!(
        compare_memory_limits(&limited(4, 512), &host_ram(2048)),
        MemoryLimitsFit::Fit {
            total_mb: 2048,
            machine_mb: 2048
        }
    );
}

#[test]
fn no_memory_limit_has_no_sum() {
    let profile = WorkerProfile {
        isolated: isolated_unlimited(4),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(1)),
        MemoryLimitsFit::NotSet
    );
}

#[test]
fn the_sum_covers_shared_async_children() {
    let profile = WorkerProfile {
        isolated: isolated_limited(2, 256),
        shared_async: shared_async(3, 10, memory_limit(1000)),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(3511)),
        MemoryLimitsFit::ExceedMachine {
            total_mb: 3512,
            machine_mb: 3511
        }
    );
}

#[test]
fn shared_children_with_no_memory_limit_have_no_sum() {
    let profile = WorkerProfile {
        isolated: isolated_limited(2, 256),
        shared_async: shared_async(1, 10, MemoryLimitSetting::NoMemoryLimit {}),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(1)),
        MemoryLimitsFit::NotSet
    );
}

#[test]
fn the_sum_covers_shared_thread_children() {
    let profile = WorkerProfile {
        isolated: isolated_limited(1, 256),
        shared_thread: shared_thread(2, 8, memory_limit(700)),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(1655)),
        MemoryLimitsFit::ExceedMachine {
            total_mb: 1656,
            machine_mb: 1655
        }
    );
}

#[test]
fn thread_children_with_no_memory_limit_have_no_sum() {
    let profile = WorkerProfile {
        isolated: isolated_limited(2, 256),
        shared_thread: shared_thread(1, 4, MemoryLimitSetting::NoMemoryLimit {}),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(1_000_000)),
        MemoryLimitsFit::NotSet
    );
}

#[test]
fn only_shared_children_are_summed() {
    let profile = WorkerProfile {
        shared_async: shared_async(4, 10, memory_limit(500)),
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(2000)),
        MemoryLimitsFit::Fit {
            total_mb: 2000,
            machine_mb: 2000
        }
    );
}

/// Python `test_no_isolated_children_has_no_memory_sum_to_exceed`: a profile
/// with no children sums to 0.
#[test]
fn no_isolated_children_has_no_memory_sum_to_exceed() {
    let profile = WorkerProfile {
        isolated: IsolatedSetting::NoIsolatedChildren {},
        ..WorkerProfile::default()
    };
    assert_eq!(
        compare_memory_limits(&profile, &host_ram(1)),
        MemoryLimitsFit::Fit {
            total_mb: 0,
            machine_mb: 1
        }
    );
}

// ---------------------------------------------------------------------------
// The cgroup files (Python `_CgroupFiles`)
// ---------------------------------------------------------------------------

/// Fake `/proc/self/cgroup` and cgroup v2 and v1 trees in a directory of
/// its own, removed on drop. `proc` does not exist until a test writes it:
/// the reader then falls back to the limit files at the mount points.
struct CgroupFiles {
    root: PathBuf,
    proc: PathBuf,
    v2_root: PathBuf,
    v1_root: PathBuf,
    v2: PathBuf,
    v1: PathBuf,
}

impl CgroupFiles {
    fn new() -> CgroupFiles {
        let root = std::env::temp_dir().join(format!("horsies-cgroup-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create the test directory");
        let v2_root = root.join("v2");
        let v1_root = root.join("v1");
        CgroupFiles {
            proc: root.join("proc").join("cgroup"),
            v2: v2_root.join(CGROUP_V2_LIMIT_FILE),
            v1: v1_root.join(CGROUP_V1_LIMIT_FILE),
            v2_root,
            v1_root,
            root,
        }
    }

    fn v2_group(&self, path: &str) -> PathBuf {
        self.v2_root.join(path).join(CGROUP_V2_LIMIT_FILE)
    }

    fn v1_group(&self, path: &str) -> PathBuf {
        self.v1_root.join(path).join(CGROUP_V1_LIMIT_FILE)
    }

    fn write(&self, path: &Path, content: &[u8]) {
        let parent = path.parent().expect("a test file has a parent directory");
        fs::create_dir_all(parent).expect("create the parent directory");
        fs::write(path, content).expect("write the test file");
    }

    fn read(&self) -> CgroupMemoryLimit {
        read_cgroup_memory_limit(&CgroupRoots {
            proc_self_cgroup: self.proc.clone(),
            v2_root: self.v2_root.clone(),
            v1_memory_root: self.v1_root.clone(),
        })
    }
}

impl Drop for CgroupFiles {
    fn drop(&mut self) {
        let _removed = fs::remove_dir_all(&self.root);
    }
}

fn limit(limit_bytes: u64) -> CgroupMemoryLimit {
    CgroupMemoryLimit::Limit { limit_bytes }
}

// TestReadCgroupMemoryLimit

#[test]
fn v2_number_is_the_limit() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v2, b"536870912\n");
    assert_eq!(cgroup.read(), limit(536_870_912));
}

#[test]
fn v2_max_is_no_limit() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v2, b"max\n");
    assert_eq!(cgroup.read(), CgroupMemoryLimit::NoLimit);
}

#[test]
fn v2_is_read_before_v1() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v2, b"max\n");
    cgroup.write(&cgroup.v1, b"536870912\n");
    assert_eq!(cgroup.read(), CgroupMemoryLimit::NoLimit);
}

#[test]
fn v1_number_is_the_limit_when_v2_is_absent() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v1, b"1073741824\n");
    assert_eq!(cgroup.read(), limit(1_073_741_824));
}

#[test]
fn no_file_is_no_limit() {
    let cgroup = CgroupFiles::new();
    assert_eq!(cgroup.read(), CgroupMemoryLimit::NoLimit);
}

#[test]
fn unexpected_content_is_unreadable() {
    for content in ["", "lots", "-1", "12 MB", "1e9"] {
        let cgroup = CgroupFiles::new();
        cgroup.write(&cgroup.v2, content.as_bytes());
        assert_eq!(
            cgroup.read(),
            CgroupMemoryLimit::Unreadable {
                reason: format!("{}: unexpected content '{content}'", cgroup.v2.display()),
            },
            "{content:?}"
        );
    }
}

#[test]
fn a_file_that_cannot_be_opened_is_unreadable() {
    let cgroup = CgroupFiles::new();
    fs::create_dir_all(&cgroup.v2).expect("a directory in place of the limit file");

    match cgroup.read() {
        CgroupMemoryLimit::Unreadable { reason } => {
            assert!(
                reason.starts_with(&format!("{}: ", cgroup.v2.display())),
                "{reason}"
            );
        }
        other => panic!("expected an unreadable limit, got {other:?}"),
    }
}

#[test]
fn a_file_above_the_read_bound_is_unreadable() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v2, "1".repeat(CGROUP_FILE_MAX_BYTES + 1).as_bytes());
    assert_eq!(
        cgroup.read(),
        CgroupMemoryLimit::Unreadable {
            reason: format!(
                "{}: more than {CGROUP_FILE_MAX_BYTES} bytes",
                cgroup.v2.display()
            ),
        }
    );
}

/// Python reads 64 digits as one integer. The core's limit is a `u64`, so
/// the file at the bound is a number padded with leading zeros; a number
/// above `u64::MAX` is unreadable, which falls back to the host RAM as a
/// limit above any RAM would.
#[test]
fn a_file_at_the_read_bound_is_read() {
    let cgroup = CgroupFiles::new();
    let padded = format!("{:0>width$}", GIB, width = CGROUP_FILE_MAX_BYTES);
    assert_eq!(padded.len(), CGROUP_FILE_MAX_BYTES);
    cgroup.write(&cgroup.v2, padded.as_bytes());
    assert_eq!(cgroup.read(), limit(GIB));

    let too_large = CgroupFiles::new();
    too_large.write(&too_large.v2, "1".repeat(CGROUP_FILE_MAX_BYTES).as_bytes());
    assert!(matches!(
        too_large.read(),
        CgroupMemoryLimit::Unreadable { .. }
    ));
}

#[test]
fn non_ascii_content_is_unreadable() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.v1, b"\xff\xfe");
    assert_eq!(
        cgroup.read(),
        CgroupMemoryLimit::Unreadable {
            reason: format!("{}: not ASCII text", cgroup.v1.display()),
        }
    );
}

// TestParseProcCgroup

fn in_cgroup(parts: &[&str]) -> CgroupPath {
    CgroupPath::InCgroup(parts.iter().map(|part| (*part).to_owned()).collect())
}

#[test]
fn the_v2_line_names_the_unified_group() {
    let parsed = parse_proc_cgroup("0::/system.slice/worker.service\n");

    assert_eq!(
        parsed,
        ProcCgroup {
            v2: in_cgroup(&["system.slice", "worker.service"]),
            v1_memory: CgroupPath::NoCgroupPath,
        }
    );
    assert_eq!(
        parsed,
        parse_proc_cgroup("0::/system.slice//worker.service/\n")
    );
}

#[test]
fn a_v1_line_with_the_memory_controller_names_the_memory_group() {
    let parsed = parse_proc_cgroup(
        "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n1:name=systemd:/docker/abc\n0::/docker/abc\n",
    );

    assert_eq!(parsed.v1_memory, in_cgroup(&["docker", "abc"]));
}

#[test]
fn a_path_that_cannot_be_walked_names_no_group() {
    let too_deep = format!("0::/{}", vec!["g"; CGROUP_PATH_MAX_DEPTH + 1].join("/"));
    for line in ["0::/../../user.slice", too_deep.as_str(), "not a line"] {
        assert_eq!(
            parse_proc_cgroup(line).v2,
            CgroupPath::NoCgroupPath,
            "{line}"
        );
    }
}

// TestCgroupOfThisProcess: `/proc/self/cgroup` names the group; the limit
// can sit on the group or on a parent group, below the mount point.

#[test]
fn a_systemd_memory_max_of_the_service_is_the_limit() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/system.slice/worker.service\n");
    cgroup.write(
        &cgroup.v2_group("system.slice/worker.service"),
        b"536870912\n",
    );

    assert_eq!(cgroup.read(), limit(536_870_912));
}

#[test]
fn a_limit_of_a_parent_slice_holds_for_the_group() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/workers.slice/worker.service\n");
    cgroup.write(&cgroup.v2_group("workers.slice/worker.service"), b"max\n");
    cgroup.write(
        &cgroup.v2_group("workers.slice"),
        format!("{GIB}\n").as_bytes(),
    );

    assert_eq!(cgroup.read(), limit(GIB));
}

#[test]
fn the_smallest_limit_on_the_path_holds() {
    // (the service, the slice, the limit): the parent is smaller; the group
    // is smaller.
    for (service, slice, expected) in [(2 * GIB, GIB, GIB), (GIB / 2, GIB, GIB / 2)] {
        let cgroup = CgroupFiles::new();
        cgroup.write(&cgroup.proc, b"0::/workers.slice/worker.service\n");
        cgroup.write(
            &cgroup.v2_group("workers.slice/worker.service"),
            format!("{service}\n").as_bytes(),
        );
        cgroup.write(
            &cgroup.v2_group("workers.slice"),
            format!("{slice}\n").as_bytes(),
        );

        assert_eq!(cgroup.read(), limit(expected), "{service} under {slice}");
    }
}

#[test]
fn a_container_seen_with_the_host_cgroup_namespace() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/system.slice/docker-abc.scope\n");
    cgroup.write(
        &cgroup.v2_group("system.slice/docker-abc.scope"),
        format!("{GIB}\n").as_bytes(),
    );

    assert_eq!(cgroup.read(), limit(GIB));
}

#[test]
fn a_v1_memory_group_is_read_before_v2() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"4:memory:/docker/abc\n0::/docker/abc\n");
    cgroup.write(
        &cgroup.v1_group("docker/abc"),
        format!("{GIB}\n").as_bytes(),
    );
    cgroup.write(&cgroup.v2_group("docker/abc"), b"max\n");

    assert_eq!(cgroup.read(), limit(GIB));
}

/// A container on cgroup v1 sees its own group at the mount point, while
/// `/proc/self/cgroup` names its path on the host.
#[test]
fn a_v1_group_not_mounted_inside_reads_the_mount_point() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"4:memory:/docker/abc\n");
    cgroup.write(&cgroup.v1, format!("{GIB}\n").as_bytes());

    assert_eq!(cgroup.read(), limit(GIB));
}

#[test]
fn a_path_that_leaves_the_mount_point_reads_the_mount_point() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/../outside\n");
    cgroup.write(&cgroup.v2, format!("{GIB}\n").as_bytes());

    assert_eq!(cgroup.read(), limit(GIB));
}

#[test]
fn an_unreadable_file_on_the_path_is_unreadable() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/workers.slice/worker.service\n");
    cgroup.write(&cgroup.v2_group("workers.slice"), b"lots\n");

    assert_eq!(
        cgroup.read(),
        CgroupMemoryLimit::Unreadable {
            reason: format!(
                "{}: unexpected content 'lots'",
                cgroup.v2_group("workers.slice").display()
            ),
        }
    );
}

#[test]
fn a_group_with_no_limit_file_on_its_path_has_no_limit() {
    let cgroup = CgroupFiles::new();
    cgroup.write(&cgroup.proc, b"0::/user.slice/session.scope\n");

    assert_eq!(cgroup.read(), CgroupMemoryLimit::NoLimit);
}

// ---------------------------------------------------------------------------
// TestUsableMemory
// ---------------------------------------------------------------------------

fn host(mb: u64) -> Result<MachineMemory, MachineMemoryUnreadable> {
    Ok(MachineMemory { mb })
}

fn host_unreadable() -> Result<MachineMemory, MachineMemoryUnreadable> {
    Err(MachineMemoryUnreadable {
        reason: "no /proc".to_owned(),
    })
}

#[test]
fn a_container_limit_below_the_host_ram_wins() {
    assert_eq!(
        usable_memory(&host(16_384), &limit(2048 * MIB)),
        Ok(UsableMemory {
            mb: 2048,
            source: MemorySource::ContainerLimit
        })
    );
}

#[test]
fn a_container_limit_equal_to_the_host_ram_is_host_ram() {
    assert_eq!(
        usable_memory(&host(2048), &limit(2048 * MIB)),
        Ok(host_ram(2048))
    );
}

#[test]
fn the_v1_no_limit_value_is_host_ram() {
    assert_eq!(
        usable_memory(&host(16_384), &limit(V1_NO_LIMIT)),
        Ok(host_ram(16_384))
    );
}

#[test]
fn no_cgroup_limit_is_host_ram() {
    assert_eq!(
        usable_memory(&host(16_384), &CgroupMemoryLimit::NoLimit),
        Ok(host_ram(16_384))
    );
}

#[test]
fn an_unreadable_cgroup_falls_back_to_host_ram() {
    let unreadable = CgroupMemoryLimit::Unreadable {
        reason: "denied".to_owned(),
    };
    assert_eq!(
        usable_memory(&host(16_384), &unreadable),
        Ok(host_ram(16_384))
    );
}

#[test]
fn a_container_limit_stands_when_the_host_ram_is_unreadable() {
    assert_eq!(
        usable_memory(&host_unreadable(), &limit(512 * MIB)),
        Ok(UsableMemory {
            mb: 512,
            source: MemorySource::ContainerLimit
        })
    );
}

#[test]
fn nothing_readable_is_unreadable() {
    for cgroup in [
        CgroupMemoryLimit::NoLimit,
        CgroupMemoryLimit::Unreadable {
            reason: "denied".to_owned(),
        },
    ] {
        assert_eq!(
            usable_memory(&host_unreadable(), &cgroup),
            Err(UsableMemoryUnreadable {
                reason: "no /proc".to_owned()
            }),
            "{cgroup:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// TestReadMachineMemory: the system reader gives the total in bytes, 0 when
// it could not read it
// ---------------------------------------------------------------------------

#[test]
fn total_bytes_become_mb() {
    assert_eq!(
        machine_memory_of(3 * GIB + 5),
        Ok(MachineMemory { mb: 3072 })
    );
}

#[test]
fn no_total_is_unreadable() {
    assert_eq!(
        machine_memory_of(0),
        Err(MachineMemoryUnreadable {
            reason: "the host reported no RAM size".to_owned()
        })
    );
}

// ---------------------------------------------------------------------------
// TestBootMemoryFit: the boot records; the worker starts in every case (the
// boot step returns records, never an error)
// ---------------------------------------------------------------------------

fn warnings(records: &[BootMemoryLog]) -> Vec<&str> {
    records
        .iter()
        .filter(|record| record.level == BootLogLevel::Warning)
        .map(|record| record.message.as_str())
        .collect()
}

#[test]
fn limits_above_the_host_ram_warn_and_the_worker_starts() {
    let fit = boot_memory_fit(&limited(4, 512), &host(1000), CgroupMemoryLimit::NoLimit);

    assert_eq!(
        warnings(&boot_memory_log("default", &fit)),
        vec![
            "Worker profile 'default': the per-child memory limits add up to 2048 MB, above \
             the 1000 MB host RAM. Children can use more memory than the worker has before \
             they are recycled."
        ]
    );
}

/// 16 GiB host, 1 GiB container: the container limit is the bound.
#[test]
fn limits_above_the_container_limit_warn_and_name_it() {
    let fit = boot_memory_fit(&limited(4, 512), &host(16_384), limit(GIB));

    assert_eq!(
        warnings(&boot_memory_log("default", &fit)),
        vec![
            "Worker profile 'default': the per-child memory limits add up to 2048 MB, above \
             the 1024 MB container limit. Children can use more memory than the worker has \
             before they are recycled."
        ]
    );
}

#[test]
fn an_unreadable_container_limit_warns_and_uses_the_host_ram() {
    let fit = boot_memory_fit(
        &limited(2, 512),
        &host(4096),
        CgroupMemoryLimit::Unreadable {
            reason: "/sys/fs/cgroup/memory.max: PermissionDenied: denied".to_owned(),
        },
    );
    let records = boot_memory_log("default", &fit);

    assert_eq!(
        warnings(&records),
        vec![
            "Worker profile 'default': the container memory limit could not be read \
             (/sys/fs/cgroup/memory.max: PermissionDenied: denied); the per-child memory \
             limits are compared with the host RAM"
        ]
    );
    assert!(records.iter().any(|record| record
        .message
        .contains("add up to 1024 MB of the 4096 MB host RAM")));
}

#[test]
fn limits_within_the_ram_do_not_warn() {
    let fit = boot_memory_fit(&limited(2, 512), &host(1024), CgroupMemoryLimit::NoLimit);
    let records = boot_memory_log("default", &fit);

    assert!(warnings(&records).is_empty(), "{records:?}");
    assert!(records.iter().any(|record| record
        .message
        .contains("add up to 1024 MB of the 1024 MB host RAM")));
}

#[test]
fn unreadable_ram_warns_and_the_worker_starts() {
    let unreadable = Err(MachineMemoryUnreadable {
        reason: "no RAM size".to_owned(),
    });
    let fit = boot_memory_fit(&limited(2, 512), &unreadable, CgroupMemoryLimit::NoLimit);
    let records = boot_memory_log("default", &fit);

    let warned = warnings(&records);
    assert_eq!(warned.len(), 1, "{records:?}");
    assert!(warned[0].contains("the memory of the machine could not be read (no RAM size)"));
}

#[test]
fn no_memory_limit_has_nothing_to_compare() {
    let profile = WorkerProfile {
        isolated: isolated_unlimited(2),
        ..WorkerProfile::default()
    };
    let fit = boot_memory_fit(&profile, &host(1), CgroupMemoryLimit::NoLimit);
    let records = boot_memory_log("default", &fit);

    assert!(warnings(&records).is_empty(), "{records:?}");
    assert!(records
        .iter()
        .any(|record| record.message.contains("no per-child memory limit")));
}
