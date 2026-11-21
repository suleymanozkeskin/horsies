//! TLS settings of a connection URL and the rustls connector built from them.
//!
//! The six libpq `sslmode` values, as libpq applies them:
//!
//! | `sslmode` | Order of attempts | Server certificate check |
//! |---|---|---|
//! | `disable` | plain only | none |
//! | `allow` | plain; TLS when the server refuses plain | the chain when a root file exists, else none |
//! | `prefer` (default) | TLS; plain when TLS fails or the server refuses the TLS session | the chain when a root file exists, else none |
//! | `require` | TLS only | the chain when a root file exists, else none |
//! | `verify-ca` | TLS only | the chain; the root file must exist |
//! | `verify-full` | TLS only | the chain and the host name; the root file must exist |
//!
//! The root file is `sslrootcert`, or libpq's default `~/.postgresql/root.crt`
//! when the URL does not name one. `sslrootcert=system` loads the platform
//! trust store; it makes `verify-full` the default and refuses every weaker
//! `sslmode`. The client certificate and key are `sslcert` and `sslkey`, or
//! libpq's defaults `~/.postgresql/postgresql.crt` and
//! `~/.postgresql/postgresql.key`.
//!
//! Where the core differs from libpq, it refuses by name:
//! - A file that the URL names and that cannot be read is an error (libpq
//!   goes on without it in some modes).
//! - A default CRL file `~/.postgresql/root.crl` next to a root file is an
//!   error: libpq would apply it, and the core does not support CRLs.
//! - An encrypted key is an error (`sslpassword` is not supported).
//!
//! [`TlsSettings::resolve`] is pure (URL parsing). [`load_tls`] does the file
//! I/O: [`probe_default_tls_files`] looks for the default files, the pure
//! [`plan_tls`] applies the rules, and the connector reads the chosen files.

use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{VerifierBuilderError, WebPkiServerVerifier};
use rustls::crypto::{
    verify_tls12_signature, verify_tls13_signature, CryptoProvider, WebPkiSupportedAlgorithms,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::environment::HomeDirectory;

/// The largest certificate or key file the core reads.
pub const MAX_TLS_FILE_BYTES: u64 = 1024 * 1024;

/// The most certificates the core takes from one PEM file.
pub const MAX_CERTIFICATES_PER_FILE: usize = 4096;

/// libpq's default root certificate file, under the home directory.
pub const DEFAULT_ROOT_CERTIFICATE_FILE: &str = ".postgresql/root.crt";

/// libpq's default certificate revocation list, under the home directory.
pub const DEFAULT_ROOT_CRL_FILE: &str = ".postgresql/root.crl";

/// libpq's default client certificate, under the home directory.
pub const DEFAULT_CLIENT_CERTIFICATE_FILE: &str = ".postgresql/postgresql.crt";

/// libpq's default client key, under the home directory.
pub const DEFAULT_CLIENT_KEY_FILE: &str = ".postgresql/postgresql.key";

/// The ALPN protocol name of PostgreSQL. libpq 17 and later always send it;
/// direct TLS negotiation needs it.
const POSTGRESQL_ALPN: &[u8] = b"postgresql";

/// The `sslmode` URL parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    /// The libpq spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Allow => "allow",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }

    /// Parses the libpq spelling.
    pub(crate) fn parse(text: &str) -> Option<SslMode> {
        match text {
            "disable" => Some(SslMode::Disable),
            "allow" => Some(SslMode::Allow),
            "prefer" => Some(SslMode::Prefer),
            "require" => Some(SslMode::Require),
            "verify-ca" => Some(SslMode::VerifyCa),
            "verify-full" => Some(SslMode::VerifyFull),
            _ => None,
        }
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether the URL set `sslmode`. libpq's defaults depend on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslModeSource {
    Given(SslMode),
    Default,
}

/// The `sslrootcert` URL parameter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootCertificates {
    /// Not set, or empty: libpq's default file `~/.postgresql/root.crt`.
    Default,
    File(PathBuf),
    /// `sslrootcert=system`: the platform trust store.
    System,
}

/// The `sslcert` or `sslkey` URL parameter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientFile {
    /// Not set, or empty: libpq's default file under `~/.postgresql/`.
    Default,
    File(PathBuf),
}

/// The `sslnegotiation` URL parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsNegotiation {
    /// The SSLRequest message first (all server versions).
    Postgres,
    /// A TLS handshake at once (PostgreSQL 17 and later).
    Direct,
}

/// The order of plain and TLS attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptOrder {
    /// `allow`: plain first; TLS when the server refuses plain.
    PlainThenTls,
    /// `prefer`: TLS first; plain when TLS fails or is refused.
    TlsThenPlain,
    /// `require`, `verify-ca`, `verify-full`.
    TlsOnly,
}

/// Root certificates from a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileRoots {
    /// libpq's default file `~/.postgresql/root.crt`.
    Default,
    File(PathBuf),
}

/// Root certificates for `verify-full`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StrictRoots {
    Files(FileRoots),
    System,
}

/// The resolved `sslmode` with its root source. Combinations that libpq
/// refuses cannot be built.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    /// `allow`, `prefer`, `require`: the chain is checked when a root file
    /// exists.
    Weak {
        order: AttemptOrder,
        roots: FileRoots,
    },
    /// `verify-ca`.
    VerifyChain {
        roots: FileRoots,
    },
    /// `verify-full`.
    VerifyChainAndHostName {
        roots: StrictRoots,
    },
}

impl TlsMode {
    /// The libpq `sslmode` this mode stands for.
    pub fn sslmode(&self) -> SslMode {
        match self {
            TlsMode::Disabled => SslMode::Disable,
            TlsMode::Weak {
                order: AttemptOrder::PlainThenTls,
                ..
            } => SslMode::Allow,
            TlsMode::Weak {
                order: AttemptOrder::TlsThenPlain,
                ..
            } => SslMode::Prefer,
            TlsMode::Weak {
                order: AttemptOrder::TlsOnly,
                ..
            } => SslMode::Require,
            TlsMode::VerifyChain { .. } => SslMode::VerifyCa,
            TlsMode::VerifyChainAndHostName { .. } => SslMode::VerifyFull,
        }
    }
}

/// The TLS settings of one URL, as parsed. No file is read yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsSettings {
    mode: TlsMode,
    certificate: ClientFile,
    key: ClientFile,
    negotiation: TlsNegotiation,
}

/// A combination of TLS parameters that libpq refuses.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TlsPolicyError {
    #[error("sslmode={sslmode} may not be used with sslrootcert=system; use sslmode=verify-full")]
    WeakSslModeWithSystemRoots { sslmode: SslMode },
    #[error(
        "sslnegotiation=direct needs sslmode require, verify-ca or verify-full, not {sslmode}"
    )]
    DirectNegotiationNeedsRequiredTls { sslmode: SslMode },
}

/// Whether the negotiation fits the mode: direct TLS needs TLS-only
/// attempts, as libpq requires.
enum NegotiationCheck {
    Allowed,
    NeedsRequiredTls,
}

/// `sslrootcert` without its `system` case.
enum RootChoice {
    System,
    Files(FileRoots),
}

impl TlsSettings {
    /// Applies libpq's rules that need no file to the TLS parameters of one
    /// URL.
    ///
    /// Errors: [`TlsPolicyError`] for a combination that libpq refuses.
    pub fn resolve(
        sslmode: SslModeSource,
        roots: RootCertificates,
        certificate: ClientFile,
        key: ClientFile,
        negotiation: TlsNegotiation,
    ) -> Result<TlsSettings, TlsPolicyError> {
        let mode = resolve_mode(sslmode, roots)?;
        let negotiation_check = match (negotiation, &mode) {
            (TlsNegotiation::Postgres, _)
            | (
                TlsNegotiation::Direct,
                TlsMode::Weak {
                    order: AttemptOrder::TlsOnly,
                    ..
                }
                | TlsMode::VerifyChain { .. }
                | TlsMode::VerifyChainAndHostName { .. },
            ) => NegotiationCheck::Allowed,
            (
                TlsNegotiation::Direct,
                TlsMode::Disabled
                | TlsMode::Weak {
                    order: AttemptOrder::PlainThenTls | AttemptOrder::TlsThenPlain,
                    ..
                },
            ) => NegotiationCheck::NeedsRequiredTls,
        };
        match negotiation_check {
            NegotiationCheck::Allowed => Ok(TlsSettings {
                mode,
                certificate,
                key,
                negotiation,
            }),
            NegotiationCheck::NeedsRequiredTls => {
                Err(TlsPolicyError::DirectNegotiationNeedsRequiredTls {
                    sslmode: mode.sslmode(),
                })
            }
        }
    }

    pub fn mode(&self) -> &TlsMode {
        &self.mode
    }

    pub fn certificate(&self) -> &ClientFile {
        &self.certificate
    }

    pub fn key(&self) -> &ClientFile {
        &self.key
    }

    pub fn negotiation(&self) -> TlsNegotiation {
        self.negotiation
    }
}

fn resolve_mode(
    sslmode: SslModeSource,
    roots: RootCertificates,
) -> Result<TlsMode, TlsPolicyError> {
    use SslMode::{Allow, Disable, Prefer, Require, VerifyCa, VerifyFull};
    use SslModeSource::{Default, Given};

    let choice = match roots {
        RootCertificates::System => RootChoice::System,
        RootCertificates::Default => RootChoice::Files(FileRoots::Default),
        RootCertificates::File(path) => RootChoice::Files(FileRoots::File(path)),
    };
    match (sslmode, choice) {
        (Default | Given(VerifyFull), RootChoice::System) => Ok(TlsMode::VerifyChainAndHostName {
            roots: StrictRoots::System,
        }),
        (Given(mode), RootChoice::System) => {
            Err(TlsPolicyError::WeakSslModeWithSystemRoots { sslmode: mode })
        }
        (Given(Disable), RootChoice::Files(_)) => Ok(TlsMode::Disabled),
        (Default | Given(Prefer), RootChoice::Files(roots)) => Ok(TlsMode::Weak {
            order: AttemptOrder::TlsThenPlain,
            roots,
        }),
        (Given(Allow), RootChoice::Files(roots)) => Ok(TlsMode::Weak {
            order: AttemptOrder::PlainThenTls,
            roots,
        }),
        (Given(Require), RootChoice::Files(roots)) => Ok(TlsMode::Weak {
            order: AttemptOrder::TlsOnly,
            roots,
        }),
        (Given(VerifyCa), RootChoice::Files(roots)) => Ok(TlsMode::VerifyChain { roots }),
        (Given(VerifyFull), RootChoice::Files(roots)) => Ok(TlsMode::VerifyChainAndHostName {
            roots: StrictRoots::Files(roots),
        }),
    }
}

/// Where the trusted root certificates come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootSource {
    File(PathBuf),
    System,
}

/// What the client checks in the server certificate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerVerification {
    /// Nothing; the connection is encrypted but the server is not
    /// authenticated.
    NotVerified,
    /// The chain against the root file; the host name is not checked.
    ChainOnly { root_file: PathBuf },
    /// The chain and the host name.
    ChainAndHostName(RootSource),
}

/// The order of plain and TLS attempts, and the certificate check of the TLS
/// attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsPolicy {
    Disabled,
    PlainThenTls(ServerVerification),
    TlsThenPlain(ServerVerification),
    TlsOnly(ServerVerification),
}

/// The client certificate the TLS attempt presents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientIdentity {
    NoCertificate,
    CertificateAndKey { certificate: PathBuf, key: PathBuf },
}

/// One libpq default file, as found on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DefaultFile {
    Present(PathBuf),
    Absent(PathBuf),
    /// The home directory is unknown, so there is no default path.
    NoHome,
}

/// The four libpq default TLS files, as found on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefaultTlsFiles {
    pub root_certificate: DefaultFile,
    pub root_crl: DefaultFile,
    pub client_certificate: DefaultFile,
    pub client_key: DefaultFile,
}

/// The files and the attempt order a URL's TLS uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsPlan {
    pub policy: TlsPolicy,
    pub identity: ClientIdentity,
}

/// The TLS file an error refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsFile {
    RootCertificates,
    RootRevocationList,
    ClientCertificate,
    ClientKey,
}

impl fmt::Display for TlsFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            TlsFile::RootCertificates => "root certificate (sslrootcert)",
            TlsFile::RootRevocationList => "certificate revocation list",
            TlsFile::ClientCertificate => "client certificate (sslcert)",
            TlsFile::ClientKey => "client key (sslkey)",
        })
    }
}

/// The TLS files cannot be used, or the rustls configuration cannot be built.
#[derive(Debug, thiserror::Error)]
pub enum TlsLoadError {
    #[error(
        "sslmode={sslmode} needs a root certificate: sslrootcert is not set and {} does not \
         exist; give sslrootcert, use sslrootcert=system, or use a weaker sslmode",
        .path.display()
    )]
    DefaultRootCertificateMissing { sslmode: SslMode, path: PathBuf },
    #[error(
        "sslmode={sslmode} needs a root certificate: sslrootcert is not set and the home \
         directory is unknown; give sslrootcert or use sslrootcert=system"
    )]
    RootCertificateNeedsHome { sslmode: SslMode },
    #[error(
        "{} exists and libpq would check the server against it; the core does not support \
         certificate revocation lists, so remove or rename the file",
        .path.display()
    )]
    DefaultCrlPresent { path: PathBuf },
    #[error(
        "the client certificate {} exists and its key {} does not; give sslkey or add the key",
        .certificate.display(),
        .key.display()
    )]
    ClientKeyMissing { certificate: PathBuf, key: PathBuf },
    #[error(
        "the client certificate {} has no key: sslkey is not set and the home directory is \
         unknown",
        .certificate.display()
    )]
    ClientKeyNeedsHome { certificate: PathBuf },
    #[error("reading the {file} file {} failed: {cause}", .path.display())]
    ReadFailed {
        file: TlsFile,
        path: PathBuf,
        cause: std::io::Error,
    },
    #[error("the {file} file {} is larger than {limit} bytes", .path.display())]
    FileTooLarge {
        file: TlsFile,
        path: PathBuf,
        limit: u64,
    },
    #[error("the {file} file {} is not valid PEM: {cause}", .path.display())]
    PemInvalid {
        file: TlsFile,
        path: PathBuf,
        cause: rustls::pki_types::pem::Error,
    },
    #[error("the {file} file {} holds no certificate", .path.display())]
    NoCertificate { file: TlsFile, path: PathBuf },
    #[error("the {file} file {} holds more than {limit} certificates", .path.display())]
    TooManyCertificates {
        file: TlsFile,
        path: PathBuf,
        limit: usize,
    },
    #[error(
        "the client key file {} holds no unencrypted private key (encrypted keys need \
         sslpassword, which the core does not support)",
        .path.display()
    )]
    NoPrivateKey { path: PathBuf },
    #[error(
        "the client key file {} has group or world access (mode {mode:o}); use u=rw (0600) or \
         less",
        .path.display()
    )]
    KeyFileTooOpen { path: PathBuf, mode: u32 },
    #[error("rustls refused a certificate of the {file} file {}: {cause}", .path.display())]
    CertificateRefused {
        file: TlsFile,
        path: PathBuf,
        cause: rustls::Error,
    },
    #[error(
        "the platform trust store gave no usable root certificate ({load_errors} load errors)"
    )]
    NoSystemRootCertificates { load_errors: usize },
    #[error("the server certificate verifier could not be built: {cause}")]
    VerifierRefused { cause: VerifierBuilderError },
    #[error("rustls refused the client certificate and key: {cause}")]
    ClientIdentityRefused { cause: rustls::Error },
    #[error("rustls refused the protocol versions: {cause}")]
    ProtocolVersionsRefused { cause: rustls::Error },
}

/// The TLS attempt order and the rustls connector of one URL.
pub(crate) struct LoadedTls {
    pub(crate) policy: TlsPolicy,
    pub(crate) connector: MakeRustlsConnect,
}

/// Builds the TLS side of one URL (blocking file I/O). With `sslmode=disable`
/// nothing is read.
pub(crate) fn load_tls(
    settings: &TlsSettings,
    home: &HomeDirectory,
) -> Result<LoadedTls, TlsLoadError> {
    let plan = match settings.mode {
        TlsMode::Disabled => disabled_plan(),
        TlsMode::Weak { .. }
        | TlsMode::VerifyChain { .. }
        | TlsMode::VerifyChainAndHostName { .. } => {
            plan_tls(settings, &probe_default_tls_files(home)?)?
        }
    };
    let connector = build_connector(&plan)?;
    Ok(LoadedTls {
        policy: plan.policy,
        connector,
    })
}

fn disabled_plan() -> TlsPlan {
    TlsPlan {
        policy: TlsPolicy::Disabled,
        identity: ClientIdentity::NoCertificate,
    }
}

/// Looks for libpq's four default TLS files under the home directory.
pub fn probe_default_tls_files(home: &HomeDirectory) -> Result<DefaultTlsFiles, TlsLoadError> {
    let HomeDirectory::Known(home) = home else {
        return Ok(DefaultTlsFiles {
            root_certificate: DefaultFile::NoHome,
            root_crl: DefaultFile::NoHome,
            client_certificate: DefaultFile::NoHome,
            client_key: DefaultFile::NoHome,
        });
    };
    Ok(DefaultTlsFiles {
        root_certificate: probe(
            home.join(DEFAULT_ROOT_CERTIFICATE_FILE),
            TlsFile::RootCertificates,
        )?,
        root_crl: probe(
            home.join(DEFAULT_ROOT_CRL_FILE),
            TlsFile::RootRevocationList,
        )?,
        client_certificate: probe(
            home.join(DEFAULT_CLIENT_CERTIFICATE_FILE),
            TlsFile::ClientCertificate,
        )?,
        client_key: probe(home.join(DEFAULT_CLIENT_KEY_FILE), TlsFile::ClientKey)?,
    })
}

/// A missing file or a missing directory on the path means absent, as libpq
/// reads `ENOENT` and `ENOTDIR`; any other error is refused.
fn probe(path: PathBuf, file: TlsFile) -> Result<DefaultFile, TlsLoadError> {
    match std::fs::metadata(&path) {
        Ok(_) => Ok(DefaultFile::Present(path)),
        Err(cause)
            if matches!(
                cause.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(DefaultFile::Absent(path))
        }
        Err(cause) => Err(TlsLoadError::ReadFailed { file, path, cause }),
    }
}

/// What the TLS attempt must check, before the root file is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequiredCheck {
    /// Weak modes: the chain when a root file exists.
    ChainWhenRootFileExists,
    Chain,
    ChainAndHostName,
}

/// Applies libpq's file rules. Pure: the default files were probed before.
pub fn plan_tls(
    settings: &TlsSettings,
    defaults: &DefaultTlsFiles,
) -> Result<TlsPlan, TlsLoadError> {
    let sslmode = settings.mode.sslmode();
    let (order, verification) = match &settings.mode {
        TlsMode::Disabled => return Ok(disabled_plan()),
        TlsMode::Weak { order, roots } => (
            *order,
            verification_from_files(
                roots,
                RequiredCheck::ChainWhenRootFileExists,
                defaults,
                sslmode,
            )?,
        ),
        TlsMode::VerifyChain { roots } => (
            AttemptOrder::TlsOnly,
            verification_from_files(roots, RequiredCheck::Chain, defaults, sslmode)?,
        ),
        TlsMode::VerifyChainAndHostName {
            roots: StrictRoots::System,
        } => (
            AttemptOrder::TlsOnly,
            ServerVerification::ChainAndHostName(RootSource::System),
        ),
        TlsMode::VerifyChainAndHostName {
            roots: StrictRoots::Files(roots),
        } => (
            AttemptOrder::TlsOnly,
            verification_from_files(roots, RequiredCheck::ChainAndHostName, defaults, sslmode)?,
        ),
    };
    refuse_default_crl(&verification, &defaults.root_crl)?;
    let identity = client_identity(&settings.certificate, &settings.key, defaults)?;
    let policy = match order {
        AttemptOrder::PlainThenTls => TlsPolicy::PlainThenTls(verification),
        AttemptOrder::TlsThenPlain => TlsPolicy::TlsThenPlain(verification),
        AttemptOrder::TlsOnly => TlsPolicy::TlsOnly(verification),
    };
    Ok(TlsPlan { policy, identity })
}

fn verification_from_files(
    roots: &FileRoots,
    required: RequiredCheck,
    defaults: &DefaultTlsFiles,
    sslmode: SslMode,
) -> Result<ServerVerification, TlsLoadError> {
    let root_file = match (roots, &defaults.root_certificate) {
        (FileRoots::File(path), _) | (FileRoots::Default, DefaultFile::Present(path)) => {
            RootFile::Found(path.clone())
        }
        (FileRoots::Default, DefaultFile::Absent(path)) => RootFile::Missing(path.clone()),
        (FileRoots::Default, DefaultFile::NoHome) => RootFile::NoHome,
    };
    match (required, root_file) {
        (
            RequiredCheck::ChainWhenRootFileExists | RequiredCheck::Chain,
            RootFile::Found(root_file),
        ) => Ok(ServerVerification::ChainOnly { root_file }),
        (RequiredCheck::ChainAndHostName, RootFile::Found(root_file)) => Ok(
            ServerVerification::ChainAndHostName(RootSource::File(root_file)),
        ),
        (RequiredCheck::ChainWhenRootFileExists, RootFile::Missing(_) | RootFile::NoHome) => {
            Ok(ServerVerification::NotVerified)
        }
        (RequiredCheck::Chain | RequiredCheck::ChainAndHostName, RootFile::Missing(path)) => {
            Err(TlsLoadError::DefaultRootCertificateMissing { sslmode, path })
        }
        (RequiredCheck::Chain | RequiredCheck::ChainAndHostName, RootFile::NoHome) => {
            Err(TlsLoadError::RootCertificateNeedsHome { sslmode })
        }
    }
}

/// The root file of a check that uses files.
enum RootFile {
    Found(PathBuf),
    Missing(PathBuf),
    NoHome,
}

/// libpq loads `~/.postgresql/root.crl` whenever it loads a root file; the
/// core does not support CRLs, so it refuses instead of ignoring the file.
fn refuse_default_crl(
    verification: &ServerVerification,
    root_crl: &DefaultFile,
) -> Result<(), TlsLoadError> {
    match (verification, root_crl) {
        (
            ServerVerification::ChainOnly { .. }
            | ServerVerification::ChainAndHostName(RootSource::File(_)),
            DefaultFile::Present(path),
        ) => Err(TlsLoadError::DefaultCrlPresent { path: path.clone() }),
        (
            ServerVerification::NotVerified
            | ServerVerification::ChainAndHostName(RootSource::System),
            _,
        )
        | (_, DefaultFile::Absent(_) | DefaultFile::NoHome) => Ok(()),
    }
}

/// libpq's rule: the certificate is `sslcert` or the default file; without a
/// certificate no key is read. With a certificate, the key is `sslkey` or the
/// default file, and it must exist. A named certificate that cannot be read
/// is refused when it is read.
fn client_identity(
    certificate: &ClientFile,
    key: &ClientFile,
    defaults: &DefaultTlsFiles,
) -> Result<ClientIdentity, TlsLoadError> {
    let certificate = match (certificate, &defaults.client_certificate) {
        (ClientFile::File(path), _) | (ClientFile::Default, DefaultFile::Present(path)) => {
            path.clone()
        }
        (ClientFile::Default, DefaultFile::Absent(_) | DefaultFile::NoHome) => {
            return Ok(ClientIdentity::NoCertificate)
        }
    };
    let key = match (key, &defaults.client_key) {
        (ClientFile::File(path), _) | (ClientFile::Default, DefaultFile::Present(path)) => {
            path.clone()
        }
        (ClientFile::Default, DefaultFile::Absent(path)) => {
            return Err(TlsLoadError::ClientKeyMissing {
                certificate,
                key: path.clone(),
            })
        }
        (ClientFile::Default, DefaultFile::NoHome) => {
            return Err(TlsLoadError::ClientKeyNeedsHome { certificate })
        }
    };
    Ok(ClientIdentity::CertificateAndKey { certificate, key })
}

/// Builds the rustls connector of a plan. Reads the chosen files.
///
/// The crypto provider is *ring*, passed explicitly, so no process-wide
/// default provider is needed or installed.
fn build_connector(plan: &TlsPlan) -> Result<MakeRustlsConnect, TlsLoadError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = server_verifier(verification_of(&plan.policy), &provider)?;
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|cause| TlsLoadError::ProtocolVersionsRefused { cause })?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut config = match &plan.identity {
        ClientIdentity::NoCertificate => builder.with_no_client_auth(),
        ClientIdentity::CertificateAndKey { certificate, key } => {
            let chain = read_certificates(certificate, TlsFile::ClientCertificate)?;
            let private_key = read_private_key(key)?;
            builder
                .with_client_auth_cert(chain, private_key)
                .map_err(|cause| TlsLoadError::ClientIdentityRefused { cause })?
        }
    };
    config.alpn_protocols = vec![POSTGRESQL_ALPN.to_vec()];
    Ok(MakeRustlsConnect::new(config))
}

/// The certificate check of the TLS attempt. A disabled policy never makes a
/// TLS attempt; its connector still needs a verifier, and gets the one that
/// checks nothing.
fn verification_of(policy: &TlsPolicy) -> &ServerVerification {
    const NOT_VERIFIED: &ServerVerification = &ServerVerification::NotVerified;
    match policy {
        TlsPolicy::Disabled => NOT_VERIFIED,
        TlsPolicy::PlainThenTls(verification)
        | TlsPolicy::TlsThenPlain(verification)
        | TlsPolicy::TlsOnly(verification) => verification,
    }
}

fn server_verifier(
    verification: &ServerVerification,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ServerCertVerifier>, TlsLoadError> {
    match verification {
        ServerVerification::NotVerified => Ok(Arc::new(AcceptAnyServerCertificate {
            algorithms: provider.signature_verification_algorithms,
        })),
        ServerVerification::ChainOnly { root_file } => {
            let roots = roots_from_file(root_file)?;
            let inner = webpki_verifier(roots, provider)?;
            Ok(Arc::new(ChainOnlyVerifier { inner }))
        }
        ServerVerification::ChainAndHostName(source) => {
            let roots = match source {
                RootSource::File(root_file) => roots_from_file(root_file)?,
                RootSource::System => system_roots()?,
            };
            let verifier: Arc<dyn ServerCertVerifier> = webpki_verifier(roots, provider)?;
            Ok(verifier)
        }
    }
}

fn webpki_verifier(
    roots: RootCertStore,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<WebPkiServerVerifier>, TlsLoadError> {
    WebPkiServerVerifier::builder_with_provider(Arc::new(roots), Arc::clone(provider))
        .build()
        .map_err(|cause| TlsLoadError::VerifierRefused { cause })
}

fn roots_from_file(path: &Path) -> Result<RootCertStore, TlsLoadError> {
    let mut roots = RootCertStore::empty();
    for certificate in read_certificates(path, TlsFile::RootCertificates)? {
        roots
            .add(certificate)
            .map_err(|cause| TlsLoadError::CertificateRefused {
                file: TlsFile::RootCertificates,
                path: path.to_path_buf(),
                cause,
            })?;
    }
    Ok(roots)
}

fn system_roots() -> Result<RootCertStore, TlsLoadError> {
    let loaded = rustls_native_certs::load_native_certs();
    let load_errors = loaded.errors.len();
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(loaded.certs);
    match added {
        0 => Err(TlsLoadError::NoSystemRootCertificates { load_errors }),
        _ => Ok(roots),
    }
}

/// Every certificate of a PEM file, in order. At least one, at most
/// [`MAX_CERTIFICATES_PER_FILE`].
fn read_certificates(
    path: &Path,
    file: TlsFile,
) -> Result<Vec<CertificateDer<'static>>, TlsLoadError> {
    let bytes = read_bounded(path, file)?;
    let mut certificates = Vec::new();
    for item in CertificateDer::pem_slice_iter(&bytes) {
        if certificates.len() == MAX_CERTIFICATES_PER_FILE {
            return Err(TlsLoadError::TooManyCertificates {
                file,
                path: path.to_path_buf(),
                limit: MAX_CERTIFICATES_PER_FILE,
            });
        }
        let certificate = item.map_err(|cause| TlsLoadError::PemInvalid {
            file,
            path: path.to_path_buf(),
            cause,
        })?;
        certificates.push(certificate);
    }
    match certificates.is_empty() {
        true => Err(TlsLoadError::NoCertificate {
            file,
            path: path.to_path_buf(),
        }),
        false => Ok(certificates),
    }
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsLoadError> {
    check_key_permissions(path)?;
    let bytes = read_bounded(path, TlsFile::ClientKey)?;
    PrivateKeyDer::from_pem_slice(&bytes).map_err(|cause| match cause {
        rustls::pki_types::pem::Error::NoItemsFound => TlsLoadError::NoPrivateKey {
            path: path.to_path_buf(),
        },
        other => TlsLoadError::PemInvalid {
            file: TlsFile::ClientKey,
            path: path.to_path_buf(),
            cause: other,
        },
    })
}

/// libpq's rule: a key file owned by the current user must have no group or
/// world access (0600 or less); one owned by root may be group-readable (0640
/// or less).
#[cfg(unix)]
fn check_key_permissions(path: &Path) -> Result<(), TlsLoadError> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = std::fs::metadata(path).map_err(|cause| TlsLoadError::ReadFailed {
        file: TlsFile::ClientKey,
        path: path.to_path_buf(),
        cause,
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    let forbidden_bits = match metadata.uid() {
        0 => 0o037,
        _ => 0o077,
    };
    match mode & forbidden_bits {
        0 => Ok(()),
        _ => Err(TlsLoadError::KeyFileTooOpen {
            path: path.to_path_buf(),
            mode,
        }),
    }
}

/// Platforms without Unix modes have no permission rule in libpq either.
#[cfg(not(unix))]
fn check_key_permissions(_path: &Path) -> Result<(), TlsLoadError> {
    Ok(())
}

/// Reads at most [`MAX_TLS_FILE_BYTES`] of a file. A longer file is refused.
fn read_bounded(path: &Path, file: TlsFile) -> Result<Vec<u8>, TlsLoadError> {
    let read_failed = |cause: std::io::Error| TlsLoadError::ReadFailed {
        file,
        path: path.to_path_buf(),
        cause,
    };
    let handle = std::fs::File::open(path).map_err(read_failed)?;
    let mut bytes = Vec::new();
    let _length = handle
        .take(MAX_TLS_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(read_failed)?;
    match u64::try_from(bytes.len()) {
        Ok(length) if length <= MAX_TLS_FILE_BYTES => Ok(bytes),
        Ok(_) | Err(_) => Err(TlsLoadError::FileTooLarge {
            file,
            path: path.to_path_buf(),
            limit: MAX_TLS_FILE_BYTES,
        }),
    }
}

/// Accepts every server certificate, and still verifies the handshake
/// signatures. Used where libpq does not check the certificate.
#[derive(Debug)]
struct AcceptAnyServerCertificate {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for AcceptAnyServerCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// Verifies the chain against the roots and accepts any host name
/// (`verify-ca`). rustls checks the chain before the name, so a name error
/// means the chain passed.
#[derive(Debug)]
struct ChainOnlyVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            Err(other) => Err(other),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(sslmode: SslModeSource, roots: RootCertificates) -> TlsSettings {
        TlsSettings::resolve(
            sslmode,
            roots,
            ClientFile::Default,
            ClientFile::Default,
            TlsNegotiation::Postgres,
        )
        .expect("valid TLS settings")
    }

    fn no_default_files() -> DefaultTlsFiles {
        DefaultTlsFiles {
            root_certificate: DefaultFile::Absent(PathBuf::from("/h/.postgresql/root.crt")),
            root_crl: DefaultFile::Absent(PathBuf::from("/h/.postgresql/root.crl")),
            client_certificate: DefaultFile::Absent(PathBuf::from("/h/.postgresql/postgresql.crt")),
            client_key: DefaultFile::Absent(PathBuf::from("/h/.postgresql/postgresql.key")),
        }
    }

    fn with_default_root() -> DefaultTlsFiles {
        DefaultTlsFiles {
            root_certificate: DefaultFile::Present(PathBuf::from("/h/.postgresql/root.crt")),
            ..no_default_files()
        }
    }

    fn no_home() -> DefaultTlsFiles {
        DefaultTlsFiles {
            root_certificate: DefaultFile::NoHome,
            root_crl: DefaultFile::NoHome,
            client_certificate: DefaultFile::NoHome,
            client_key: DefaultFile::NoHome,
        }
    }

    fn policy(sslmode: SslMode, defaults: &DefaultTlsFiles) -> Result<TlsPolicy, TlsLoadError> {
        plan_tls(
            &settings(SslModeSource::Given(sslmode), RootCertificates::Default),
            defaults,
        )
        .map(|plan| plan.policy)
    }

    fn planned(sslmode: SslMode, defaults: &DefaultTlsFiles) -> TlsPolicy {
        match policy(sslmode, defaults) {
            Ok(policy) => policy,
            Err(error) => panic!("{sslmode}: expected a plan, got {error}"),
        }
    }

    fn default_root_chain() -> ServerVerification {
        ServerVerification::ChainOnly {
            root_file: PathBuf::from("/h/.postgresql/root.crt"),
        }
    }

    #[test]
    fn the_default_sslmode_is_prefer_and_system_roots_mean_verify_full() {
        assert_eq!(
            settings(SslModeSource::Default, RootCertificates::Default).mode(),
            &TlsMode::Weak {
                order: AttemptOrder::TlsThenPlain,
                roots: FileRoots::Default
            },
        );
        assert_eq!(
            settings(SslModeSource::Default, RootCertificates::System).mode(),
            &TlsMode::VerifyChainAndHostName {
                roots: StrictRoots::System
            },
        );
    }

    #[test]
    fn system_roots_refuse_every_weaker_sslmode() {
        for mode in [
            SslMode::Disable,
            SslMode::Allow,
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
        ] {
            assert_eq!(
                TlsSettings::resolve(
                    SslModeSource::Given(mode),
                    RootCertificates::System,
                    ClientFile::Default,
                    ClientFile::Default,
                    TlsNegotiation::Postgres,
                ),
                Err(TlsPolicyError::WeakSslModeWithSystemRoots { sslmode: mode }),
            );
        }
    }

    #[test]
    fn direct_negotiation_needs_tls_only() {
        assert_eq!(
            TlsSettings::resolve(
                SslModeSource::Default,
                RootCertificates::Default,
                ClientFile::Default,
                ClientFile::Default,
                TlsNegotiation::Direct,
            ),
            Err(TlsPolicyError::DirectNegotiationNeedsRequiredTls {
                sslmode: SslMode::Prefer
            }),
        );
        assert!(TlsSettings::resolve(
            SslModeSource::Given(SslMode::Require),
            RootCertificates::Default,
            ClientFile::Default,
            ClientFile::Default,
            TlsNegotiation::Direct,
        )
        .is_ok());
    }

    #[test]
    fn weak_modes_without_a_root_file_check_nothing() {
        assert_eq!(
            planned(SslMode::Allow, &no_default_files()),
            TlsPolicy::PlainThenTls(ServerVerification::NotVerified)
        );
        assert_eq!(
            planned(SslMode::Prefer, &no_home()),
            TlsPolicy::TlsThenPlain(ServerVerification::NotVerified)
        );
        assert_eq!(
            planned(SslMode::Require, &no_default_files()),
            TlsPolicy::TlsOnly(ServerVerification::NotVerified)
        );
        assert_eq!(
            planned(SslMode::Disable, &with_default_root()),
            TlsPolicy::Disabled
        );
    }

    #[test]
    fn weak_modes_check_the_chain_when_the_default_root_file_exists() {
        assert_eq!(
            planned(SslMode::Require, &with_default_root()),
            TlsPolicy::TlsOnly(default_root_chain())
        );
        assert_eq!(
            planned(SslMode::Prefer, &with_default_root()),
            TlsPolicy::TlsThenPlain(default_root_chain())
        );
        assert_eq!(
            planned(SslMode::Allow, &with_default_root()),
            TlsPolicy::PlainThenTls(default_root_chain())
        );
    }

    #[test]
    fn verify_modes_read_the_default_root_file() {
        assert_eq!(
            planned(SslMode::VerifyCa, &with_default_root()),
            TlsPolicy::TlsOnly(default_root_chain())
        );
        assert_eq!(
            planned(SslMode::VerifyFull, &with_default_root()),
            TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(RootSource::File(
                PathBuf::from("/h/.postgresql/root.crt")
            )))
        );
    }

    #[test]
    fn verify_modes_refuse_a_missing_default_root_file_by_name() {
        for mode in [SslMode::VerifyCa, SslMode::VerifyFull] {
            match policy(mode, &no_default_files()) {
                Err(TlsLoadError::DefaultRootCertificateMissing { sslmode, path }) => {
                    assert_eq!(sslmode, mode);
                    assert_eq!(path, PathBuf::from("/h/.postgresql/root.crt"));
                }
                other => panic!("{mode}: expected DefaultRootCertificateMissing, got {other:?}"),
            }
            assert!(matches!(
                policy(mode, &no_home()),
                Err(TlsLoadError::RootCertificateNeedsHome { .. })
            ));
        }
    }

    #[test]
    fn a_named_root_file_is_used_in_every_mode() {
        let named = RootCertificates::File(PathBuf::from("/certs/root.crt"));
        let chain = ServerVerification::ChainOnly {
            root_file: PathBuf::from("/certs/root.crt"),
        };
        for (mode, expected) in [
            (SslMode::Require, TlsPolicy::TlsOnly(chain.clone())),
            (SslMode::VerifyCa, TlsPolicy::TlsOnly(chain.clone())),
            (
                SslMode::VerifyFull,
                TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(RootSource::File(
                    PathBuf::from("/certs/root.crt"),
                ))),
            ),
        ] {
            let plan = plan_tls(
                &settings(SslModeSource::Given(mode), named.clone()),
                &no_home(),
            )
            .expect("a named root file needs no home");
            assert_eq!(plan.policy, expected, "{mode}");
        }
    }

    #[test]
    fn system_roots_need_no_file() {
        let plan = plan_tls(
            &settings(SslModeSource::Default, RootCertificates::System),
            &no_home(),
        )
        .expect("system roots");
        assert_eq!(
            plan.policy,
            TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(RootSource::System)),
        );
    }

    #[test]
    fn a_default_crl_next_to_a_root_file_is_refused() {
        let defaults = DefaultTlsFiles {
            root_crl: DefaultFile::Present(PathBuf::from("/h/.postgresql/root.crl")),
            ..with_default_root()
        };
        assert!(matches!(
            policy(SslMode::Require, &defaults),
            Err(TlsLoadError::DefaultCrlPresent { .. })
        ));
        let no_root = DefaultTlsFiles {
            root_crl: DefaultFile::Present(PathBuf::from("/h/.postgresql/root.crl")),
            ..no_default_files()
        };
        assert_eq!(
            planned(SslMode::Require, &no_root),
            TlsPolicy::TlsOnly(ServerVerification::NotVerified)
        );
    }

    #[test]
    fn the_default_client_certificate_is_used_with_its_key() {
        let defaults = DefaultTlsFiles {
            client_certificate: DefaultFile::Present(PathBuf::from(
                "/h/.postgresql/postgresql.crt",
            )),
            client_key: DefaultFile::Present(PathBuf::from("/h/.postgresql/postgresql.key")),
            ..no_default_files()
        };
        let plan = plan_tls(
            &settings(
                SslModeSource::Given(SslMode::Require),
                RootCertificates::Default,
            ),
            &defaults,
        )
        .expect("plan");
        assert_eq!(
            plan.identity,
            ClientIdentity::CertificateAndKey {
                certificate: PathBuf::from("/h/.postgresql/postgresql.crt"),
                key: PathBuf::from("/h/.postgresql/postgresql.key"),
            },
        );
    }

    #[test]
    fn a_client_certificate_without_its_key_is_refused() {
        let defaults = DefaultTlsFiles {
            client_certificate: DefaultFile::Present(PathBuf::from(
                "/h/.postgresql/postgresql.crt",
            )),
            ..no_default_files()
        };
        assert!(matches!(
            plan_tls(
                &settings(
                    SslModeSource::Given(SslMode::Require),
                    RootCertificates::Default
                ),
                &defaults,
            ),
            Err(TlsLoadError::ClientKeyMissing { .. })
        ));
    }

    #[test]
    fn a_key_without_a_certificate_is_not_read() {
        let named_key = TlsSettings::resolve(
            SslModeSource::Given(SslMode::Require),
            RootCertificates::Default,
            ClientFile::Default,
            ClientFile::File(PathBuf::from("/keys/client.key")),
            TlsNegotiation::Postgres,
        )
        .expect("valid settings");
        let plan = plan_tls(&named_key, &no_default_files()).expect("plan");
        assert_eq!(plan.identity, ClientIdentity::NoCertificate);
    }

    #[test]
    fn sslmode_spellings_round_trip() {
        for mode in [
            SslMode::Disable,
            SslMode::Allow,
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
            SslMode::VerifyFull,
        ] {
            assert_eq!(SslMode::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(SslMode::parse("verify_full"), None);
    }

    #[test]
    fn a_disabled_mode_reads_no_file() {
        let disabled = settings(
            SslModeSource::Given(SslMode::Disable),
            RootCertificates::File(PathBuf::from("/no/such/root.crt")),
        );
        let loaded = load_tls(
            &disabled,
            &HomeDirectory::Known(PathBuf::from("/no/such/home")),
        )
        .expect("nothing is read");
        assert_eq!(loaded.policy, TlsPolicy::Disabled);
    }

    #[test]
    fn a_missing_named_root_file_is_a_read_error() {
        let named = settings(
            SslModeSource::Given(SslMode::VerifyCa),
            RootCertificates::File(PathBuf::from("/no/such/root.crt")),
        );
        match load_tls(&named, &HomeDirectory::Unknown) {
            Err(TlsLoadError::ReadFailed {
                file: TlsFile::RootCertificates,
                ..
            }) => {}
            Err(other) => panic!("expected ReadFailed for the root file, got {other}"),
            Ok(_) => panic!("expected ReadFailed for the root file, got a connector"),
        }
    }

    #[test]
    fn probing_an_empty_home_finds_every_default_file_absent() {
        let home = std::env::temp_dir().join(format!("stmt_home_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&home).expect("create temp home");
        let probed = probe_default_tls_files(&HomeDirectory::Known(home.clone()));
        let _removed = std::fs::remove_dir_all(&home);
        let probed = probed.expect("probe");
        assert!(matches!(probed.root_certificate, DefaultFile::Absent(_)));
        assert!(matches!(probed.root_crl, DefaultFile::Absent(_)));
        assert!(matches!(probed.client_certificate, DefaultFile::Absent(_)));
        assert!(matches!(probed.client_key, DefaultFile::Absent(_)));
    }

    #[test]
    fn a_root_file_without_certificates_is_refused() {
        let directory = std::env::temp_dir().join(format!("stmt_tls_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("create temp dir");
        let path = directory.join("empty.crt");
        std::fs::write(&path, b"not a pem file\n").expect("write temp file");
        let outcome = read_certificates(&path, TlsFile::RootCertificates);
        let _removed = std::fs::remove_dir_all(&directory);
        match outcome {
            Err(TlsLoadError::NoCertificate {
                file: TlsFile::RootCertificates,
                ..
            }) => {}
            Err(other) => panic!("expected NoCertificate, got {other}"),
            Ok(certificates) => panic!("expected NoCertificate, got {}", certificates.len()),
        }
    }
}
