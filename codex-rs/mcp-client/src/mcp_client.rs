//! Client utilities for interacting with Model Context Protocol (MCP) servers.
//!
//! This module wraps the `rmcp` SDK so Codex can rely on the upstream
//! JSON-RPC handling, transports, and helpers instead of maintaining bespoke
//! plumbing.

use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use rmcp::ErrorData as McpError;
use rmcp::model::CallToolRequest;
use rmcp::model::CallToolRequestParam;
use rmcp::model::ClientCapabilities;
use rmcp::model::ClientInfo;
use rmcp::model::ClientNotification;
use rmcp::model::ClientRequest;
use rmcp::model::ClientResult;
use rmcp::model::CreateElicitationResult;
use rmcp::model::CreateMessageRequestMethod;
use rmcp::model::ElicitationAction;
use rmcp::model::ElicitationCapability;
use rmcp::model::Implementation;
use rmcp::model::JsonObject;
use rmcp::model::ListRootsResult;
use rmcp::model::ProtocolVersion;
use rmcp::model::ServerNotification;
use rmcp::model::ServerRequest;
use rmcp::model::ServerResult;
use rmcp::service::NotificationContext;
use rmcp::service::Peer;
use rmcp::service::PeerRequestOptions;
use rmcp::service::RequestContext;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::service::Service;
use rmcp::service::ServiceExt;
use rmcp::transport::ConfigureCommandExt;
use rmcp::transport::IntoTransport;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::TokioChildProcess;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::debug;

const NOTIFICATION_CHANNEL_CAPACITY: usize = 128;

/// A running MCP client backed by the upstream `rmcp` SDK.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<McpClientInner>,
}

struct McpClientInner {
    running_service: RunningService<RoleClient, CodexClientService>,
    notifications: broadcast::Sender<ServerNotification>,
}

impl McpClient {
    /// Spawn an MCP server as a subprocess and connect over stdio.
    pub async fn spawn_stdio(
        program: OsString,
        args: Vec<OsString>,
        env: Option<HashMap<String, String>>,
        timeout_duration: Duration,
    ) -> Result<Self> {
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(create_env_for_mcp_server(env));

        let transport = TokioChildProcess::new(command.configure(|cmd| {
            cmd.stdin(std::process::Stdio::piped());
            cmd.stdout(std::process::Stdio::piped());
            cmd.stderr(std::process::Stdio::inherit());
            cmd.kill_on_drop(true);
        }))?;

        Self::start_with_transport(transport, timeout_duration).await
    }

    /// Connect to an MCP server over HTTP+SSE using the streamable transport.
    pub async fn connect_http(
        uri: impl Into<String>,
        client: Option<reqwest::Client>,
        headers: Option<reqwest::header::HeaderMap>,
        transport_config: Option<StreamableHttpClientTransportConfig>,
        timeout_duration: Duration,
    ) -> Result<Self> {
        use reqwest::header::AUTHORIZATION;
        use std::sync::Arc as StdArc;

        let uri_string = uri.into();
        let uri_arc: StdArc<str> = StdArc::from(uri_string.as_str());

        let mut config = transport_config
            .unwrap_or_else(|| StreamableHttpClientTransportConfig::with_uri(uri_arc.clone()));
        config.uri = uri_arc.clone();

        if let Some(value) = headers
            .as_ref()
            .and_then(|map| map.get(AUTHORIZATION))
            .and_then(|value| value.to_str().ok())
        {
            config = config.auth_header(value.to_owned());
        }

        let client = match client {
            Some(existing) => existing,
            None => {
                let mut builder = reqwest::Client::builder();
                if let Some(headers) = headers.clone() {
                    builder = builder.default_headers(headers);
                }
                builder.build().context("building reqwest client")?
            }
        };

        let transport = StreamableHttpClientTransport::with_client(client, config);
        Self::start_with_transport(transport, timeout_duration).await
    }

    /// Subscribe to notifications emitted by the connected server.
    pub fn subscribe_notifications(&self) -> broadcast::Receiver<ServerNotification> {
        self.inner.notifications.subscribe()
    }

    /// List every tool exposed by the server, automatically following pagination.
    pub async fn list_all_tools(&self) -> Result<Vec<mcp_types::Tool>> {
        let tools = self
            .peer()
            .list_all_tools()
            .await
            .map_err(|err| anyhow!(err))?;
        tools.into_iter().map(convert_value).collect()
    }

    /// Invoke a tool by name with optional JSON arguments.
    pub async fn call_tool(
        &self,
        tool_name: String,
        arguments: Option<serde_json::Value>,
        timeout_duration: Option<Duration>,
    ) -> Result<mcp_types::CallToolResult> {
        let params = CallToolRequestParam {
            name: tool_name.clone().into(),
            arguments: arguments
                .map(value_to_json_object)
                .transpose()
                .with_context(|| format!("invalid arguments for tool `{tool_name}`"))?,
        };

        let result = if let Some(timeout_duration) = timeout_duration {
            let request = CallToolRequest::new(params.clone());
            let handle = self
                .peer()
                .send_request_with_option(
                    ClientRequest::CallToolRequest(request),
                    PeerRequestOptions {
                        timeout: Some(timeout_duration),
                        meta: None,
                    },
                )
                .await
                .map_err(|err| anyhow!(err))?;
            match handle.await_response().await.map_err(|err| anyhow!(err))? {
                ServerResult::CallToolResult(result) => result,
                other => return Err(anyhow!("unexpected response variant: {other:?}")),
            }
        } else {
            self.peer()
                .call_tool(params)
                .await
                .map_err(|err| anyhow!(err))?
        };

        convert_value(result)
    }

    /// Send an arbitrary request to the server using raw MCP model types.
    pub async fn send_request(&self, request: ClientRequest) -> Result<ServerResult> {
        self.peer()
            .send_request(request)
            .await
            .map_err(|err| anyhow!(err))
    }

    /// Send a notification to the server using raw MCP model types.
    pub async fn send_notification(&self, notification: ClientNotification) -> Result<()> {
        self.peer()
            .send_notification(notification)
            .await
            .map_err(|err| anyhow!(err))
    }

    fn peer(&self) -> &Peer<RoleClient> {
        self.inner.running_service.peer()
    }

    async fn start_with_transport<T, E, A>(transport: T, timeout_duration: Duration) -> Result<Self>
    where
        T: IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CHANNEL_CAPACITY);
        let service = CodexClientService::new(notifications.clone());
        let running_service = timeout(timeout_duration, service.serve(transport))
            .await
            .map_err(|_| anyhow!("timed out waiting for MCP client initialization"))?
            .map_err(|err| anyhow!(err))?;

        Ok(Self {
            inner: Arc::new(McpClientInner {
                running_service,
                notifications,
            }),
        })
    }
}

struct CodexClientService {
    client_info: ClientInfo,
    notifications: broadcast::Sender<ServerNotification>,
}

impl CodexClientService {
    fn new(notifications: broadcast::Sender<ServerNotification>) -> Self {
        Self {
            client_info: codex_client_info(),
            notifications,
        }
    }
}

impl Service<RoleClient> for CodexClientService {
    async fn handle_request(
        &self,
        request: ServerRequest,
        _context: RequestContext<RoleClient>,
    ) -> Result<ClientResult, McpError> {
        match request {
            ServerRequest::PingRequest(_) => Ok(ClientResult::empty(())),
            ServerRequest::CreateMessageRequest(_) => {
                Err(McpError::method_not_found::<CreateMessageRequestMethod>())
            }
            ServerRequest::ListRootsRequest(_) => {
                Ok(ClientResult::ListRootsResult(ListRootsResult::default()))
            }
            ServerRequest::CreateElicitationRequest(_) => Ok(
                ClientResult::CreateElicitationResult(CreateElicitationResult {
                    action: ElicitationAction::Decline,
                    content: None,
                }),
            ),
        }
    }

    fn handle_notification(
        &self,
        notification: ServerNotification,
        _context: NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send + '_ {
        let tx = self.notifications.clone();
        async move {
            if let Err(err) = tx.send(notification) {
                debug!("dropping server notification; no active subscribers: {err}");
            }
            Ok(())
        }
    }

    fn get_info(&self) -> ClientInfo {
        self.client_info.clone()
    }
}

fn codex_client_info() -> ClientInfo {
    ClientInfo {
        protocol_version: parse_protocol_version(mcp_types::MCP_SCHEMA_VERSION),
        capabilities: ClientCapabilities {
            experimental: None,
            roots: None,
            sampling: None,
            elicitation: Some(ElicitationCapability::default()),
        },
        client_info: Implementation {
            name: "codex-mcp-client".to_owned(),
            title: Some("Codex".to_owned()),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            icons: None,
            website_url: None,
        },
    }
}

fn parse_protocol_version(version: &str) -> ProtocolVersion {
    serde_json::from_str::<ProtocolVersion>(&format!("\"{version}\""))
        .unwrap_or(ProtocolVersion::LATEST)
}

fn convert_value<T, U>(value: T) -> Result<U>
where
    T: Serialize,
    U: DeserializeOwned,
{
    let json = serde_json::to_value(value)?;
    Ok(serde_json::from_value(json)?)
}

fn value_to_json_object(value: serde_json::Value) -> Result<JsonObject> {
    match value {
        serde_json::Value::Object(map) => Ok(map),
        other => Err(anyhow!(
            "expected arguments to be a JSON object, got {other}"
        )),
    }
}

/// Environment variables that are always included when spawning a new MCP
/// server.
#[rustfmt::skip]
#[cfg(unix)]
const DEFAULT_ENV_VARS: &[&str] = &[
    "HOME",
    "LOGNAME",
    "PATH",
    "SHELL",
    "USER",
    "__CF_USER_TEXT_ENCODING",
    "LANG",
    "LC_ALL",
    "TERM",
    "TMPDIR",
    "TZ",
];

#[cfg(windows)]
const DEFAULT_ENV_VARS: &[&str] = &[
    "PATH",
    "PATHEXT",
    "USERNAME",
    "USERDOMAIN",
    "USERPROFILE",
    "TEMP",
    "TMP",
];

/// `extra_env` comes from the config for an entry in `mcp_servers` in
/// `config.toml`.
fn create_env_for_mcp_server(
    extra_env: Option<HashMap<String, String>>,
) -> HashMap<String, String> {
    DEFAULT_ENV_VARS
        .iter()
        .filter_map(|var| match std::env::var(var) {
            Ok(value) => Some((var.to_string(), value)),
            Err(_) => None,
        })
        .chain(extra_env.unwrap_or_default())
        .collect::<HashMap<_, _>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_create_env_for_mcp_server() {
        let env_var = "USER";
        let env_var_existing_value = std::env::var(env_var).unwrap_or_default();
        let env_var_new_value = format!("{env_var_existing_value}-extra");
        let extra_env = HashMap::from([(env_var.to_owned(), env_var_new_value.clone())]);
        let mcp_server_env = create_env_for_mcp_server(Some(extra_env));
        assert!(mcp_server_env.contains_key("PATH"));
        assert_eq!(Some(&env_var_new_value), mcp_server_env.get(env_var));
    }
}
