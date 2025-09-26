//! Simple command-line utility to exercise `McpClient`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use clap::ArgAction;
use clap::Parser;
use codex_mcp_client::McpClient;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "codex-mcp-client",
    about = "Interact with MCP servers over stdio or HTTP"
)]
struct Cli {
    /// Connect to an MCP server over HTTP instead of spawning a subprocess.
    #[arg(long)]
    http: Option<String>,

    /// Additional HTTP headers to include when connecting over HTTP.
    #[arg(long = "header", value_parser = parse_header, value_name = "NAME:VALUE")]
    headers: Vec<(HeaderName, HeaderValue)>,

    /// Additional environment variables to pass to the spawned MCP server.
    #[arg(long = "env", value_parser = parse_key_val, value_name = "NAME=VALUE")]
    env: Vec<(String, String)>,

    /// Timeout (seconds) for establishing the connection.
    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,

    /// Program (and optional arguments) to run when using stdio transport.
    #[arg(value_name = "PROGRAM", trailing_var_arg = true, action = ArgAction::Append)]
    args: Vec<OsString>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let timeout = Duration::from_secs(cli.timeout_secs);

    let client = if let Some(url) = cli.http {
        let mut header_map = HeaderMap::new();
        for (name, value) in cli.headers {
            header_map.insert(name, value);
        }
        McpClient::connect_http(url, None, Some(header_map), None, timeout)
            .await
            .context("failed to connect to HTTP MCP server")?
    } else {
        let mut args = cli.args;
        if args.is_empty() {
            anyhow::bail!(
                "Usage: codex-mcp-client <program> [args..]\n\nExample: codex-mcp-client codex-mcp-server"
            );
        }
        let program = args.remove(0);
        let env: HashMap<String, String> = cli.env.into_iter().collect();
        McpClient::spawn_stdio(program, args, Some(env), timeout)
            .await
            .context("failed to spawn subprocess")?
    };

    let tools = client
        .list_all_tools()
        .await
        .context("tools/list request failed")?;
    println!("{}", serde_json::to_string_pretty(&tools)?);

    Ok(())
}

fn init_tracing() {
    let default_level = "debug";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new(default_level))
                .unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    let (key, value) = s
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("expected NAME=VALUE, got {s}"))?;
    Ok((key.to_owned(), value.to_owned()))
}

fn parse_header(s: &str) -> Result<(HeaderName, HeaderValue)> {
    let (name, value) = s
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("expected NAME:VALUE, got {s}"))?;
    let header_name = HeaderName::try_from(name.trim())
        .map_err(|err| anyhow::anyhow!("invalid header name `{name}`: {err}"))?;
    let header_value = HeaderValue::try_from(value.trim())
        .map_err(|err| anyhow::anyhow!("invalid header value `{value}`: {err}"))?;
    Ok((header_name, header_value))
}
