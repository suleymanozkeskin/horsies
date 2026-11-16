//! TLS settings of a connection URL and the rustls connector built from them.
//!
//! The six libpq `sslmode` values map to a [`TlsPolicy`]:
//!
//! | `sslmode` | Order of attempts | Server certificate check |
//! |---|---|---|
//! | `disable` | plain only | none |
//! | `allow` | plain, then TLS if the server refuses plain | none; the chain when `sslrootcert` names a file |
//! | `prefer` (default) | TLS when the server offers it, else plain | none; the chain when `sslrootcert` names a file |
//! | `require` | TLS only | none; the chain when `sslrootcert` names a file |
//! | `verify-ca` | TLS only | the chain against `sslrootcert` |
//! | `verify-full` | TLS only | the chain and the host name |
//!
//! libpq's rule that `require` with a root CA file behaves as `verify-ca` is
//! applied to `allow` and `prefer` too, as libpq does. `sslrootcert=system`
//! loads the platform trust store; it makes `verify-full` the default and
//! refuses every weaker `sslmode`.
//!
//! The core reads only the files the URL names. It does not read the libpq
//! default files under `~/.postgresql/`.

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

/// The largest certificate or key file the core reads.
pub const MAX_TLS_FILE_BYTES: u64 = 1024 * 1024;

/// The most certificates the core takes from one PEM file.
pub const MAX_CERTIFICATES_PER_FILE: usize = 4096;

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
    NotGiven,
    File(PathBuf),
    /// `sslrootcert=system`: the platform trust store.
    System,
}

/// The `sslcert` and `sslkey` URL parameters, which come as a pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientIdentity {
    NotGiven,
    CertificateAndKey { certificate: PathBuf, key: PathBuf },
}

/// The `sslnegotiation` URL parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsNegotiation {
    /// The SSLRequest message first (all server versions).
    Postgres,
    /// A TLS handshake at once (PostgreSQL 17 and later).
    Direct,
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
    /// `sslmode=disable`.
    Disabled,
    /// `sslmode=allow`: plain first; TLS when the server refuses plain.
    PlainThenTls(ServerVerification),
    /// `sslmode=prefer`: TLS when the server offers it, else plain.
    TlsThenPlain(ServerVerification),
    /// `require`, `verify-ca`, `verify-full`.
    TlsOnly(ServerVerification),
}

/// The resolved TLS settings of one URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsSettings {
    policy: TlsPolicy,
    identity: ClientIdentity,
    negotiation: TlsNegotiation,
}

/// A combination of TLS parameters that libpq refuses, or that needs a file
/// the core does not read by default.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum TlsPolicyError {
    #[error(
        "sslmode={sslmode} needs sslrootcert: the core reads no default root certificate file"
    )]
    RootCertificateRequired { sslmode: SslMode },
    #[error("sslmode={sslmode} may not be used with sslrootcert=system; use sslmode=verify-full")]
    WeakSslModeWithSystemRoots { sslmode: SslMode },
    #[error(
        "sslnegotiation=direct needs sslmode require, verify-ca or verify-full, not {sslmode}"
    )]
    DirectNegotiationNeedsRequiredTls { sslmode: SslMode },
    #[error(
        "{given} is set and {missing} is not; the core reads no default client certificate or \
         key file"
    )]
    ClientIdentityIncomplete {
        given: &'static str,
        missing: &'static str,
    },
}

impl TlsSettings {
    /// Applies libpq's rules to the TLS parameters of one URL.
    ///
    /// Errors: [`TlsPolicyError`] for a combination that libpq refuses or
    /// that needs a default file.
    pub fn resolve(
        sslmode: SslModeSource,
        roots: RootCertificates,
        identity: ClientIdentity,
        negotiation: TlsNegotiation,
    ) -> Result<TlsSettings, TlsPolicyError> {
        let policy = resolve_policy(sslmode, roots)?;
        let negotiation_check = match (negotiation, &policy) {
            (TlsNegotiation::Postgres, _) | (TlsNegotiation::Direct, TlsPolicy::TlsOnly(_)) => {
                NegotiationCheck::Allowed
            }
            (
                TlsNegotiation::Direct,
                TlsPolicy::Disabled | TlsPolicy::PlainThenTls(_) | TlsPolicy::TlsThenPlain(_),
            ) => NegotiationCheck::NeedsRequiredTls,
        };
        match negotiation_check {
            NegotiationCheck::Allowed => Ok(TlsSettings {
                policy,
                identity,
                negotiation,
            }),
            NegotiationCheck::NeedsRequiredTls => {
                Err(TlsPolicyError::DirectNegotiationNeedsRequiredTls {
                    sslmode: match sslmode {
                        SslModeSource::Given(mode) => mode,
                        SslModeSource::Default => SslMode::Prefer,
                    },
                })
            }
        }
    }

    pub fn policy(&self) -> &TlsPolicy {
        &self.policy
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }

    pub fn negotiation(&self) -> TlsNegotiation {
        self.negotiation
    }
}

/// Whether the negotiation fits the policy: direct TLS needs a TLS-only
/// policy, as libpq requires.
enum NegotiationCheck {
    Allowed,
    NeedsRequiredTls,
}

fn resolve_policy(
    sslmode: SslModeSource,
    roots: RootCertificates,
) -> Result<TlsPolicy, TlsPolicyError> {
    use RootCertificates::{File, NotGiven, System};
    use SslMode::{Allow, Disable, Prefer, Require, VerifyCa, VerifyFull};
    use SslModeSource::{Default, Given};
    use TlsPolicy::{Disabled, PlainThenTls, TlsOnly, TlsThenPlain};

    match (sslmode, roots) {
        (Default | Given(VerifyFull), System) => Ok(TlsOnly(ServerVerification::ChainAndHostName(
            RootSource::System,
        ))),
        (Given(mode), System) => Err(TlsPolicyError::WeakSslModeWithSystemRoots { sslmode: mode }),
        (Given(Disable), NotGiven | File(_)) => Ok(Disabled),
        (Default | Given(Prefer), NotGiven) => Ok(TlsThenPlain(ServerVerification::NotVerified)),
        (Default | Given(Prefer), File(root_file)) => {
            Ok(TlsThenPlain(ServerVerification::ChainOnly { root_file }))
        }
        (Given(Allow), NotGiven) => Ok(PlainThenTls(ServerVerification::NotVerified)),
        (Given(Allow), File(root_file)) => {
            Ok(PlainThenTls(ServerVerification::ChainOnly { root_file }))
        }
        (Given(Require), NotGiven) => Ok(TlsOnly(ServerVerification::NotVerified)),
        (Given(Require), File(root_file)) => {
            Ok(TlsOnly(ServerVerification::ChainOnly { root_file }))
        }
        (Given(mode @ (VerifyCa | VerifyFull)), NotGiven) => {
            Err(TlsPolicyError::RootCertificateRequired { sslmode: mode })
        }
        (Given(VerifyCa), File(root_file)) => {
            Ok(TlsOnly(ServerVerification::ChainOnly { root_file }))
        }
        (Given(VerifyFull), File(root_file)) => Ok(TlsOnly(ServerVerification::ChainAndHostName(
            RootSource::File(root_file),
        ))),
    }
}

/// Pairs `sslcert` and `sslkey`. A non-empty value counts as given.
pub(crate) fn client_identity(
    certificate: Option<String>,
    key: Option<String>,
) -> Result<ClientIdentity, TlsPolicyError> {
    let certificate = certificate.filter(|text| !text.is_empty());
    let key = key.filter(|text| !text.is_empty());
    match (certificate, key) {
        (None, None) => Ok(ClientIdentity::NotGiven),
        (Some(certificate), Some(key)) => Ok(ClientIdentity::CertificateAndKey {
            certificate: PathBuf::from(certificate),
            key: PathBuf::from(key),
        }),
        (Some(_), None) => Err(TlsPolicyError::ClientIdentityIncomplete {
            given: "sslcert",
            missing: "sslkey",
        }),
        (None, Some(_)) => Err(TlsPolicyError::ClientIdentityIncomplete {
            given: "sslkey",
            missing: "sslcert",
        }),
    }
}

/// The TLS file a load error refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsFile {
    RootCertificates,
    ClientCertificate,
    ClientKey,
}

impl fmt::Display for TlsFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            TlsFile::RootCertificates => "sslrootcert",
            TlsFile::ClientCertificate => "sslcert",
            TlsFile::ClientKey => "sslkey",
        })
    }
}

/// Loading the TLS files or building the rustls configuration failed.
#[derive(Debug, thiserror::Error)]
pub enum TlsLoadError {
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
        "the sslkey file {} holds no unencrypted private key (encrypted keys need sslpassword, \
         which the core does not support)",
        .path.display()
    )]
    NoPrivateKey { path: PathBuf },
    #[error(
        "the sslkey file {} has group or world access (mode {mode:o}); use u=rw (0600) or less",
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

/// Builds the rustls connector of one URL. Reads the files that the settings
/// name (blocking file reads); a URL with TLS disabled reads nothing.
///
/// The crypto provider is *ring*, passed explicitly, so no process-wide
/// default provider is needed or installed.
pub(crate) fn load_tls_connector(
    settings: &TlsSettings,
) -> Result<MakeRustlsConnect, TlsLoadError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = server_verifier(verification_of(&settings.policy), &provider)?;
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|cause| TlsLoadError::ProtocolVersionsRefused { cause })?
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut config = match &settings.identity {
        ClientIdentity::NotGiven => builder.with_no_client_auth(),
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

    fn root_file() -> RootCertificates {
        RootCertificates::File(PathBuf::from("/certs/root.crt"))
    }

    fn resolve(
        sslmode: SslModeSource,
        roots: RootCertificates,
    ) -> Result<TlsPolicy, TlsPolicyError> {
        TlsSettings::resolve(
            sslmode,
            roots,
            ClientIdentity::NotGiven,
            TlsNegotiation::Postgres,
        )
        .map(|settings| settings.policy)
    }

    #[test]
    fn default_sslmode_is_prefer_without_verification() {
        assert_eq!(
            resolve(SslModeSource::Default, RootCertificates::NotGiven),
            Ok(TlsPolicy::TlsThenPlain(ServerVerification::NotVerified)),
        );
    }

    #[test]
    fn each_sslmode_without_root_file() {
        let cases = [
            (SslMode::Disable, Ok(TlsPolicy::Disabled)),
            (
                SslMode::Allow,
                Ok(TlsPolicy::PlainThenTls(ServerVerification::NotVerified)),
            ),
            (
                SslMode::Prefer,
                Ok(TlsPolicy::TlsThenPlain(ServerVerification::NotVerified)),
            ),
            (
                SslMode::Require,
                Ok(TlsPolicy::TlsOnly(ServerVerification::NotVerified)),
            ),
            (
                SslMode::VerifyCa,
                Err(TlsPolicyError::RootCertificateRequired {
                    sslmode: SslMode::VerifyCa,
                }),
            ),
            (
                SslMode::VerifyFull,
                Err(TlsPolicyError::RootCertificateRequired {
                    sslmode: SslMode::VerifyFull,
                }),
            ),
        ];
        for (mode, expected) in cases {
            assert_eq!(
                resolve(SslModeSource::Given(mode), RootCertificates::NotGiven),
                expected,
                "{mode}"
            );
        }
    }

    #[test]
    fn a_root_file_makes_weak_modes_check_the_chain() {
        let chain = || ServerVerification::ChainOnly {
            root_file: PathBuf::from("/certs/root.crt"),
        };
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::Require), root_file()),
            Ok(TlsPolicy::TlsOnly(chain())),
        );
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::Prefer), root_file()),
            Ok(TlsPolicy::TlsThenPlain(chain())),
        );
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::Allow), root_file()),
            Ok(TlsPolicy::PlainThenTls(chain())),
        );
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::VerifyCa), root_file()),
            Ok(TlsPolicy::TlsOnly(chain())),
        );
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::Disable), root_file()),
            Ok(TlsPolicy::Disabled),
        );
    }

    #[test]
    fn verify_full_checks_chain_and_host_name() {
        assert_eq!(
            resolve(SslModeSource::Given(SslMode::VerifyFull), root_file()),
            Ok(TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(
                RootSource::File(PathBuf::from("/certs/root.crt"))
            ))),
        );
    }

    #[test]
    fn system_roots_default_to_verify_full_and_refuse_weaker_modes() {
        assert_eq!(
            resolve(SslModeSource::Default, RootCertificates::System),
            Ok(TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(
                RootSource::System
            ))),
        );
        assert_eq!(
            resolve(
                SslModeSource::Given(SslMode::VerifyFull),
                RootCertificates::System
            ),
            Ok(TlsPolicy::TlsOnly(ServerVerification::ChainAndHostName(
                RootSource::System
            ))),
        );
        for mode in [
            SslMode::Disable,
            SslMode::Allow,
            SslMode::Prefer,
            SslMode::Require,
            SslMode::VerifyCa,
        ] {
            assert_eq!(
                resolve(SslModeSource::Given(mode), RootCertificates::System),
                Err(TlsPolicyError::WeakSslModeWithSystemRoots { sslmode: mode }),
            );
        }
    }

    #[test]
    fn direct_negotiation_needs_tls_only() {
        let refused = TlsSettings::resolve(
            SslModeSource::Default,
            RootCertificates::NotGiven,
            ClientIdentity::NotGiven,
            TlsNegotiation::Direct,
        );
        assert_eq!(
            refused,
            Err(TlsPolicyError::DirectNegotiationNeedsRequiredTls {
                sslmode: SslMode::Prefer
            }),
        );
        let accepted = TlsSettings::resolve(
            SslModeSource::Given(SslMode::Require),
            RootCertificates::NotGiven,
            ClientIdentity::NotGiven,
            TlsNegotiation::Direct,
        );
        assert!(accepted.is_ok());
    }

    #[test]
    fn client_certificate_and_key_come_as_a_pair() {
        assert_eq!(client_identity(None, None), Ok(ClientIdentity::NotGiven));
        assert_eq!(
            client_identity(Some(String::new()), Some(String::new())),
            Ok(ClientIdentity::NotGiven),
        );
        assert_eq!(
            client_identity(Some("c.crt".to_owned()), Some("c.key".to_owned())),
            Ok(ClientIdentity::CertificateAndKey {
                certificate: PathBuf::from("c.crt"),
                key: PathBuf::from("c.key"),
            }),
        );
        assert_eq!(
            client_identity(Some("c.crt".to_owned()), None),
            Err(TlsPolicyError::ClientIdentityIncomplete {
                given: "sslcert",
                missing: "sslkey"
            }),
        );
        assert_eq!(
            client_identity(None, Some("c.key".to_owned())),
            Err(TlsPolicyError::ClientIdentityIncomplete {
                given: "sslkey",
                missing: "sslcert"
            }),
        );
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
    fn a_disabled_policy_loads_no_file() {
        let settings = TlsSettings::resolve(
            SslModeSource::Given(SslMode::Disable),
            RootCertificates::File(PathBuf::from("/no/such/root.crt")),
            ClientIdentity::NotGiven,
            TlsNegotiation::Postgres,
        )
        .expect("disable with a root file is valid");
        assert!(load_tls_connector(&settings).is_ok());
    }

    #[test]
    fn a_missing_root_file_is_a_named_read_error() {
        let settings = TlsSettings::resolve(
            SslModeSource::Given(SslMode::VerifyCa),
            RootCertificates::File(PathBuf::from("/no/such/root.crt")),
            ClientIdentity::NotGiven,
            TlsNegotiation::Postgres,
        )
        .expect("verify-ca with a root file is valid");
        match load_tls_connector(&settings) {
            Err(TlsLoadError::ReadFailed {
                file: TlsFile::RootCertificates,
                ..
            }) => {}
            Err(other) => panic!("expected ReadFailed for sslrootcert, got {other}"),
            Ok(_) => panic!("expected ReadFailed for sslrootcert, got a connector"),
        }
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
