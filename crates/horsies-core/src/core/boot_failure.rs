//! The facts of a failed child boot that more than one part of the core
//! reads: the child frames and the supervisor of the executor, and the
//! `slots_down` value of a worker state snapshot (Python
//! `python/horsies/core/models/boot_failure.py`). Pure.

/// The detail of a failed boot has at most this many characters (Python
/// `MAX_BOOT_FAILURE_DETAIL_CHARS`).
pub const MAX_BOOT_FAILURE_DETAIL_CHARS: usize = 2400;

/// An errno that names an OS resource limit. A boot that failed with one can
/// succeed later, when the resource is free again (Python `ShortageErrno`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShortageErrno {
    Emfile,
    Enfile,
    Enomem,
    Eagain,
}

impl ShortageErrno {
    /// Every member, in Python's order.
    pub const ALL: [Self; 4] = [Self::Emfile, Self::Enfile, Self::Enomem, Self::Eagain];

    /// The name in frames and in stored values: `EMFILE`, `ENFILE`, `ENOMEM`
    /// or `EAGAIN`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Emfile => "EMFILE",
            Self::Enfile => "ENFILE",
            Self::Enomem => "ENOMEM",
            Self::Eagain => "EAGAIN",
        }
    }

    /// The member with this name; `None` for any other text. Each caller
    /// turns `None` into its own refusal.
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "EMFILE" => Some(Self::Emfile),
            "ENFILE" => Some(Self::Enfile),
            "ENOMEM" => Some(Self::Enomem),
            "EAGAIN" => Some(Self::Eagain),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `ShortageErrno`: four members, named by their errno.
    #[test]
    fn each_errno_reads_back_from_its_name() {
        let names: Vec<&str> = ShortageErrno::ALL
            .iter()
            .map(|errno| errno.as_str())
            .collect();
        assert_eq!(names, ["EMFILE", "ENFILE", "ENOMEM", "EAGAIN"]);
        for errno in ShortageErrno::ALL {
            assert_eq!(ShortageErrno::from_name(errno.as_str()), Some(errno));
        }
        for other in ["emfile", "EINTR", "", "EMFILE "] {
            assert_eq!(ShortageErrno::from_name(other), None, "{other:?}");
        }
    }

    #[test]
    fn the_detail_bound_is_2400_characters() {
        assert_eq!(MAX_BOOT_FAILURE_DETAIL_CHARS, 2400);
    }
}
