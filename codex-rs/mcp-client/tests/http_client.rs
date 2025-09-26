use std::time::Duration;

use anyhow::Result;
use axum::Router;
use codex_mcp_client::McpClient;
use pretty_assertions::assert_eq;
use rmcp::handler::server::ServerHandler;
use rmcp::model::CallToolRequestParam;
use rmcp::model::CallToolResult;
use rmcp::model::InitializeResult;
use rmcp::model::ListToolsResult;
use rmcp::model::Tool;
use rmcp::service::NotificationContext;
use rmcp::service::RequestContext;
use rmcp::service::RoleServer;
use rmcp::transport::streamable_http_server::StreamableHttpServerConfig;
use rmcp::transport::streamable_http_server::StreamableHttpService;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use serde_json::json;
use tokio::sync::oneshot;
use tokio::time::timeout;

#[derive(Clone, Default)]
struct TestServer;

impl ServerHandler for TestServer {
    fn get_info(&self) -> InitializeResult {
        InitializeResult {
            protocol_version: rmcp::model::ProtocolVersion::LATEST,
            capabilities: rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
            server_info: rmcp::model::Implementation {
                name: "test-server".to_string(),
                title: Some("Test Server".to_string()),
                version: "0.1.0".to_string(),
                icons: None,
                website_url: None,
            },
            instructions: None,
        }
    }

    fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        let tool: Tool = serde_json::from_value(json!({
            "name": "echo",
            "title": "Echo",
            "description": "Echo back the provided text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "text": { "type": "string" }
                },
                "required": ["text"]
            }
        }))
        .unwrap_or_else(|err| panic!("valid tool schema: {err}"));
        std::future::ready(Ok(ListToolsResult {
            tools: vec![tool],
            next_cursor: None,
        }))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParam,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let _ = context.peer.notify_tool_list_changed().await;
        let text_argument = request
            .arguments
            .and_then(|map| map.get("text").cloned())
            .and_then(|value| value.as_str().map(std::borrow::ToOwned::to_owned))
            .unwrap_or_default();
        let response: CallToolResult = serde_json::from_value(json!({
            "content": [
                {
                    "type": "text",
                    "text": format!("echo: {text_argument}")
                }
            ]
        }))
        .unwrap_or_else(|err| panic!("valid call tool result: {err}"));
        Ok(response)
    }

    fn on_progress(
        &self,
        _params: rmcp::model::ProgressNotificationParam,
        _context: NotificationContext<RoleServer>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        std::future::ready(())
    }
}

#[tokio::test]
async fn http_client_connects_and_receives_notifications() -> Result<()> {
    let service: StreamableHttpService<TestServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(TestServer),
            Default::default(),
            StreamableHttpServerConfig {
                stateful_mode: true,
                sse_keep_alive: None,
            },
        );

    let router = Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                shutdown_rx.await.expect("shutdown signal receiver error");
            })
            .await
            .expect("axum server error");
    });

    let url = format!("http://{addr}/mcp");
    let client = McpClient::connect_http(url, None, None, None, Duration::from_secs(5)).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");

    let mut notifications = client.subscribe_notifications();
    let result = client
        .call_tool(
            "echo".to_string(),
            Some(json!({ "text": "hello" })),
            Some(Duration::from_secs(5)),
        )
        .await?;
    assert_eq!(result.content.len(), 1);

    let notification = timeout(Duration::from_secs(5), notifications.recv()).await??;
    match notification {
        rmcp::model::ServerNotification::ToolListChangedNotification(_) => {}
        other => panic!("unexpected notification: {other:?}"),
    }

    drop(client);
    shutdown_tx.send(()).ok();
    server_handle.await.unwrap();
    Ok(())
}
