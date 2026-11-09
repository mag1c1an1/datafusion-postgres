pub mod auth;
pub(crate) mod client;
mod handlers;
pub mod hooks;
#[cfg(feature = "pgvector")]
pub mod pgvector;
mod planner;
#[cfg(any(test, debug_assertions))]
pub mod testing;

use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use getset::{Getters, Setters, WithSetters};
use log::{info, warn};
use pgwire::api::PgWireServerHandlers;
use pgwire::tokio::process_socket;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, ServerConfig};

use handlers::HandlerFactory;
pub use handlers::{DfSessionService, Parser};
pub use hooks::QueryHook;

/// re-exports
pub use arrow_pg;
pub use datafusion_pg_catalog;
pub use datafusion_pg_functions;
pub use pgwire;

/// TCP keepalive for accepted connections.
///
/// A client that disappears without closing its socket (a network partition,
/// a NAT that dropped the flow, a host that lost power) leaves the server
/// waiting on a read that never fails, and the connection - with everything it
/// holds - stays open indefinitely. Keepalive probes let the kernel notice and
/// fail that read. PostgreSQL enables keepalive on every TCP connection for the
/// same reason (`tcp_keepalives_idle` and friends).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpKeepalive {
    /// How long a connection is idle before the first probe.
    pub idle: Duration,
    /// Time between unanswered probes; the operating system's default when
    /// `None` or where the platform cannot set it.
    pub interval: Option<Duration>,
    /// Unanswered probes before the connection is dropped; the operating
    /// system's default when `None` or where the platform cannot set it.
    pub retries: Option<u32>,
}

impl TcpKeepalive {
    fn to_socket2(self) -> socket2::TcpKeepalive {
        let params = socket2::TcpKeepalive::new().with_time(self.idle);
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        let params = match self.interval {
            Some(interval) => params.with_interval(interval),
            None => params,
        };
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let params = match self.retries {
            Some(retries) => params.with_retries(retries),
            None => params,
        };
        params
    }
}

/// Enables `keepalive` on an accepted connection.
pub fn set_tcp_keepalive(socket: &TcpStream, keepalive: TcpKeepalive) -> std::io::Result<()> {
    socket2::SockRef::from(socket).set_tcp_keepalive(&keepalive.to_socket2())
}

#[derive(Getters, Setters, WithSetters, Debug)]
#[getset(get = "pub", set = "pub", set_with = "pub")]
pub struct ServerOptions {
    host: String,
    port: u16,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    max_connections: usize,
    /// Keepalive for accepted TCP connections; the operating system's setting
    /// (usually off) when `None`.
    tcp_keepalive: Option<TcpKeepalive>,
}

impl ServerOptions {
    pub fn new() -> ServerOptions {
        ServerOptions::default()
    }
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            host: "127.0.0.1".to_string(),
            port: 5432,
            tls_cert_path: None,
            tls_key_path: None,
            max_connections: 0, // 0 = no limit
            tcp_keepalive: None,
        }
    }
}

/// Set up TLS configuration if certificate and key paths are provided
fn setup_tls(cert_path: &str, key_path: &str) -> Result<TlsAcceptor, IOError> {
    // Install ring crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert = certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open(key_path)?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
        .into_iter()
        .next()
        .ok_or_else(|| IOError::new(ErrorKind::InvalidInput, "No private key found"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Serve the Datafusion `SessionContext` with Postgres protocol.
pub async fn serve(
    session_context: Arc<SessionContext>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error> {
    serve_with_hooks(session_context, opts, handlers::default_query_hooks()).await
}

/// Serve the Datafusion `SessionContext` with Postgres protocol, using custom
/// query processing hooks.
///
/// The optional pgvector support (the distance-operator expression planner and
/// the `INSERT` literal hook) is installed here, where the session context can
/// be modified, and appended to `hooks`.
pub async fn serve_with_hooks(
    session_context: Arc<SessionContext>,
    opts: &ServerOptions,
    hooks: Vec<Arc<dyn QueryHook>>,
) -> Result<(), std::io::Error> {
    #[cfg(feature = "postgis")]
    geodatafusion::register(&session_context);

    #[cfg(feature = "pgvector")]
    let hooks = {
        let mut hooks = hooks;
        crate::pgvector::install(&session_context).map_err(std::io::Error::other)?;
        hooks.push(Arc::new(crate::pgvector::PgVectorInsertHook));
        hooks
    };

    // Create the handler factory with authentication
    let factory = Arc::new(HandlerFactory::new_with_hooks(session_context, hooks));

    serve_with_handlers(factory, opts).await
}

/// Serve with custom pgwire handlers
///
/// This function allows you to rewrite some of the built-in logic including
/// authentication and query processing. You can Implement your own
/// `PgWireServerHandlers` by reusing `DfSessionService`.
pub async fn serve_with_handlers(
    handlers: Arc<impl PgWireServerHandlers + Sync + Send + 'static>,
    opts: &ServerOptions,
) -> Result<(), std::io::Error> {
    // Set up TLS if configured
    let tls_acceptor =
        if let (Some(cert_path), Some(key_path)) = (&opts.tls_cert_path, &opts.tls_key_path) {
            match setup_tls(cert_path, key_path) {
                Ok(acceptor) => {
                    info!("TLS enabled using cert: {cert_path} and key: {key_path}");
                    Some(acceptor)
                }
                Err(e) => {
                    warn!("Failed to setup TLS: {e}. Running without encryption.");
                    None
                }
            }
        } else {
            info!("TLS not configured. Running without encryption.");
            None
        };

    // Bind to the specified host and port
    let server_addr = format!("{}:{}", opts.host, opts.port);
    let listener = TcpListener::bind(&server_addr).await?;
    if tls_acceptor.is_some() {
        info!("Listening on {server_addr} with TLS encryption");
    } else {
        info!("Listening on {server_addr} (unencrypted)");
    }

    // Connection limiter (if configured)
    let max_conn_count = opts.max_connections;
    let connection_limiter = if max_conn_count > 0 {
        Some(Arc::new(Semaphore::new(max_conn_count)))
    } else {
        None
    };
    let tcp_keepalive = opts.tcp_keepalive;

    // Accept incoming connections
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                // A connection without keepalive still works; it only loses
                // dead-peer detection, so a failure is not a reason to refuse
                // the client.
                if let Some(keepalive) = tcp_keepalive
                    && let Err(e) = set_tcp_keepalive(&socket, keepalive)
                {
                    warn!("Failed to enable TCP keepalive for {addr}: {e}");
                }
                let factory_ref = handlers.clone();
                let tls_acceptor_ref = tls_acceptor.clone();
                let limiter_ref = connection_limiter.clone();

                tokio::spawn(async move {
                    // Check connection limit if configured
                    let _permit = if let Some(ref semaphore) = limiter_ref {
                        match semaphore.try_acquire() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                warn!(
                                    "Connection rejected from {addr}: max connections ({max_conn_count}) reached"
                                );
                                return;
                            }
                        }
                    } else {
                        None
                    };

                    if let Err(e) = process_socket(socket, tls_acceptor_ref, factory_ref).await {
                        warn!("Error processing socket from {addr}: {e}");
                    }
                    // Permit is automatically released when _permit is dropped
                });
            }
            Err(e) => {
                warn!("Error accept socket: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_options_default_max_connections() {
        let opts = ServerOptions::default();
        assert_eq!(opts.max_connections, 0); // No limit by default
    }

    #[test]
    fn test_server_options_max_connections_configuration() {
        let opts = ServerOptions::new().with_max_connections(500);
        assert_eq!(opts.max_connections, 500);

        // Test that 0 means no limit
        let opts_no_limit = ServerOptions::new().with_max_connections(0);
        assert_eq!(opts_no_limit.max_connections, 0);
    }

    #[test]
    fn test_server_options_tcp_keepalive_is_off_by_default() {
        assert_eq!(*ServerOptions::default().tcp_keepalive(), None);
    }

    /// The keepalive reaches an accepted socket with every parameter the
    /// platform can set.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_set_tcp_keepalive_configures_an_accepted_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        assert!(!socket2::SockRef::from(&accepted).keepalive().unwrap());

        let keepalive = TcpKeepalive {
            idle: Duration::from_secs(60),
            interval: Some(Duration::from_secs(10)),
            retries: Some(6),
        };
        set_tcp_keepalive(&accepted, keepalive).unwrap();

        let socket = socket2::SockRef::from(&accepted);
        assert!(socket.keepalive().unwrap());
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(60)
        );
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(10)
        );
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), 6);
    }
}
