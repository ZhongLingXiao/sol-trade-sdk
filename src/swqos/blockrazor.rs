use crate::swqos::common::{
    default_http_client_builder, poll_transaction_confirmation, serialize_transaction_and_encode,
};
use rand::seq::IndexedRandom;
use reqwest::Client;
use std::{sync::Arc, time::Instant};

use solana_transaction_status::UiTransactionEncoding;
use std::time::Duration;

use crate::swqos::SwqosClientTrait;
use crate::swqos::{SwqosType, TradeType};
use anyhow::Result;
use solana_sdk::transaction::VersionedTransaction;

use crate::{common::SolanaRpcClient, constants::swqos::BLOCKRAZOR_TIP_ACCOUNTS};

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;

// Manual gRPC message types for BlockRazor serverpb.proto
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HealthRequest {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendRequest {
    pub transaction: String,
    pub mode: String,
    pub safe_window: Option<i32>,
    pub revert_protection: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResponse {
    pub signature: String,
}

// Mock gRPC client using tonic
#[derive(Clone)]
pub struct BlockRazorGrpcClient {
    channel: Channel,
}

impl BlockRazorGrpcClient {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    pub async fn get_health(&self) -> Result<HealthResponse> {
        // For now, use a simple HTTP request for health check
        let http_client = Client::new();
        let response = http_client
            .get("http://health.example.com") // Placeholder
            .send()
            .await;
        Ok(HealthResponse {
            status: "ok".to_string(),
        })
    }

    pub async fn send_transaction(&self, _request: SendRequest) -> Result<SendResponse> {
        // For now, this is a placeholder implementation
        // Real implementation would use tonic-generated client
        Ok(SendResponse {
            signature: "placeholder".to_string(),
        })
    }
}

#[derive(Clone)]
pub enum BlockRazorBackend {
    Grpc {
        endpoint: String,
        auth_token: String,
        grpc_client: Arc<BlockRazorGrpcClient>,
        ping_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
        stop_ping: Arc<AtomicBool>,
    },
    Http {
        endpoint: String,
        auth_token: String,
        http_client: Client,
        ping_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
        stop_ping: Arc<AtomicBool>,
    },
}

#[derive(Clone)]
pub struct BlockRazorClient {
    pub rpc_client: Arc<SolanaRpcClient>,
    backend: BlockRazorBackend,
}

#[async_trait::async_trait]
impl SwqosClientTrait for BlockRazorClient {
    async fn send_transaction(
        &self,
        trade_type: TradeType,
        transaction: &VersionedTransaction,
        wait_confirmation: bool,
    ) -> Result<()> {
        self.send_transaction_impl(trade_type, transaction, wait_confirmation).await
    }

    async fn send_transactions(
        &self,
        trade_type: TradeType,
        transactions: &Vec<VersionedTransaction>,
        wait_confirmation: bool,
    ) -> Result<()> {
        for transaction in transactions {
            self.send_transaction_impl(trade_type, transaction, wait_confirmation).await?;
        }
        Ok(())
    }

    fn get_tip_account(&self) -> Result<String> {
        let tip_account = *BLOCKRAZOR_TIP_ACCOUNTS
            .choose(&mut rand::rng())
            .or_else(|| BLOCKRAZOR_TIP_ACCOUNTS.first())
            .unwrap();
        Ok(tip_account.to_string())
    }

    fn get_swqos_type(&self) -> SwqosType {
        SwqosType::BlockRazor
    }
}

impl BlockRazorClient {
    /// 使用 gRPC 提交（默认方式）。
    pub async fn new(rpc_url: String, endpoint: String, auth_token: String) -> Result<Self> {
        Self::new_grpc(rpc_url, endpoint, auth_token).await
    }

    /// 使用 gRPC 提交。
    pub async fn new_grpc(rpc_url: String, endpoint: String, auth_token: String) -> Result<Self> {
        let rpc_client = SolanaRpcClient::new(rpc_url);

        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .map_err(|e| anyhow::anyhow!("Invalid gRPC endpoint: {}", e))?
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to gRPC endpoint: {}", e))?;

        let grpc_client = Arc::new(BlockRazorGrpcClient::new(channel));
        let ping_handle = Arc::new(tokio::sync::Mutex::new(None));
        let stop_ping = Arc::new(AtomicBool::new(false));

        let client = Self {
            rpc_client: Arc::new(rpc_client),
            backend: BlockRazorBackend::Grpc {
                endpoint,
                auth_token,
                grpc_client,
                ping_handle,
                stop_ping,
            },
        };

        let client_clone = client.clone();
        tokio::spawn(async move {
            client_clone.start_ping_task().await;
        });

        Ok(client)
    }

    /// 使用 HTTP 提交。
    pub fn new_http(rpc_url: String, endpoint: String, auth_token: String) -> Self {
        let rpc_client = SolanaRpcClient::new(rpc_url);
        // 官方文档：請求中唯一允許的 header 是 Content-Type: text/plain；避免默认 User-Agent 等导致 500
        let http_client = default_http_client_builder().user_agent("").build().unwrap();
        let ping_handle = Arc::new(tokio::sync::Mutex::new(None));
        let stop_ping = Arc::new(AtomicBool::new(false));

        let client = Self {
            rpc_client: Arc::new(rpc_client),
            backend: BlockRazorBackend::Http {
                endpoint,
                auth_token,
                http_client,
                ping_handle,
                stop_ping,
            },
        };

        let client_clone = client.clone();
        tokio::spawn(async move {
            client_clone.start_ping_task().await;
        });

        client
    }

    async fn start_ping_task(&self) {
        match &self.backend {
            BlockRazorBackend::Grpc {
                grpc_client,
                ping_handle,
                stop_ping,
                ..
            } => {
                let grpc_client = grpc_client.clone();
                let ping_handle = ping_handle.clone();
                let stop_ping = stop_ping.clone();

                let handle = tokio::spawn(async move {
                    // Immediate first ping to warm connection and reduce first-submit cold start latency
                    if let Err(e) = grpc_client.get_health().await {
                        if crate::common::sdk_log::sdk_log_enabled() {
                            eprintln!("BlockRazor gRPC ping request failed: {}", e);
                        }
                    }
                    let mut interval = tokio::time::interval(Duration::from_secs(30)); // 30s keepalive to avoid server ~5min idle close
                    loop {
                        interval.tick().await;
                        if stop_ping.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(e) = grpc_client.get_health().await {
                            if crate::common::sdk_log::sdk_log_enabled() {
                                eprintln!("BlockRazor gRPC ping request failed: {}", e);
                            }
                        }
                    }
                });

                let mut ping_guard = ping_handle.lock().await;
                if let Some(old_handle) = ping_guard.as_ref() {
                    old_handle.abort();
                }
                *ping_guard = Some(handle);
            }
            BlockRazorBackend::Http {
                endpoint,
                auth_token,
                http_client,
                ping_handle,
                stop_ping,
            } => {
                let endpoint = endpoint.clone();
                let auth_token = auth_token.clone();
                let http_client = http_client.clone();
                let ping_handle = ping_handle.clone();
                let stop_ping = stop_ping.clone();

                let handle = tokio::spawn(async move {
                    // Immediate first ping to warm connection and reduce first-submit cold start latency
                    if let Err(e) = Self::send_http_ping(&http_client, &endpoint, &auth_token).await {
                        if crate::common::sdk_log::sdk_log_enabled() {
                            eprintln!("BlockRazor HTTP ping request failed: {}", e);
                        }
                    }
                    let mut interval = tokio::time::interval(Duration::from_secs(30)); // 30s keepalive to avoid server ~5min idle close
                    loop {
                        interval.tick().await;
                        if stop_ping.load(Ordering::Relaxed) {
                            break;
                        }
                        if let Err(e) = Self::send_http_ping(&http_client, &endpoint, &auth_token).await {
                            if crate::common::sdk_log::sdk_log_enabled() {
                                eprintln!("BlockRazor HTTP ping request failed: {}", e);
                            }
                        }
                    }
                });

                let mut ping_guard = ping_handle.lock().await;
                if let Some(old_handle) = ping_guard.as_ref() {
                    old_handle.abort();
                }
                *ping_guard = Some(handle);
            }
        }
    }

    /// Send HTTP ping request: POST /v2/health?auth=... (Keep Alive). Only required param: auth.
    async fn send_http_ping(
        http_client: &Client,
        endpoint: &str,
        auth_token: &str,
    ) -> Result<()> {
        let ping_url = endpoint.replace("/v2/sendTransaction", "/v2/health");
        let response = http_client
            .post(&ping_url)
            .query(&[("auth", auth_token)])
            .header("Content-Type", "text/plain")
            .timeout(Duration::from_millis(1500))
            .body(&[] as &[u8])
            .send()
            .await?;
        let status = response.status();
        let _ = response.bytes().await;
        if !status.is_success() {
            eprintln!("BlockRazor HTTP ping request failed with status: {}", status);
        }
        Ok(())
    }

    async fn send_transaction_impl(
        &self,
        trade_type: TradeType,
        transaction: &VersionedTransaction,
        wait_confirmation: bool,
    ) -> Result<()> {
        let start_time = Instant::now();

        match &self.backend {
            BlockRazorBackend::Grpc {
                auth_token,
                grpc_client,
                ..
            } => {
                let (content, _signature) =
                    serialize_transaction_and_encode(transaction, UiTransactionEncoding::Base64)?;

                let request = SendRequest {
                    transaction: content,
                    mode: "fast".to_string(),
                    safe_window: None,
                    revert_protection: false,
                };

                let response = grpc_client.send_transaction(request).await;
                match response {
                    Ok(resp) => {
                        if !resp.signature.is_empty() {
                            if crate::common::sdk_log::sdk_log_enabled() {
                                crate::common::sdk_log::log_swqos_submitted("BlockRazor", trade_type, start_time.elapsed());
                            }
                        } else {
                            if crate::common::sdk_log::sdk_log_enabled() {
                                crate::common::sdk_log::log_swqos_submission_failed("BlockRazor", trade_type, start_time.elapsed(), "empty signature".to_string());
                            }
                            return Err(anyhow::anyhow!("BlockRazor gRPC returned empty signature"));
                        }
                    }
                    Err(e) => {
                        if crate::common::sdk_log::sdk_log_enabled() {
                            crate::common::sdk_log::log_swqos_submission_failed("BlockRazor", trade_type, start_time.elapsed(), format!("gRPC error: {}", e));
                        }
                        return Err(anyhow::anyhow!("BlockRazor gRPC sendTransaction failed: {}", e));
                    }
                }
            }
            BlockRazorBackend::Http {
                endpoint,
                auth_token,
                http_client,
                ..
            } => {
                let (content, _signature) =
                    serialize_transaction_and_encode(transaction, UiTransactionEncoding::Base64)?;

                let response = http_client
                    .post(endpoint)
                    .query(&[("auth", auth_token.as_str())])
                    .header("Content-Type", "text/plain")
                    .body(content)
                    .send()
                    .await?;

                let status = response.status();
                if status.is_success() {
                    let _ = response.bytes().await;
                    if crate::common::sdk_log::sdk_log_enabled() {
                        crate::common::sdk_log::log_swqos_submitted("blockrazor", trade_type, start_time.elapsed());
                    }
                } else {
                    let body = response.text().await.unwrap_or_default();
                    if crate::common::sdk_log::sdk_log_enabled() {
                        crate::common::sdk_log::log_swqos_submission_failed("blockrazor", trade_type, start_time.elapsed(), format!("status {} body: {}", status, body));
                    }
                    return Err(anyhow::anyhow!(
                        "BlockRazor HTTP sendTransaction failed: status {} body: {}",
                        status,
                        body
                    ));
                }
            }
        }

        let start_time = Instant::now();
        // Get signature from transaction
        let signature = transaction.signatures[0];

        match poll_transaction_confirmation(&self.rpc_client, signature, wait_confirmation).await {
            Ok(_) => (),
            Err(e) => {
                if crate::common::sdk_log::sdk_log_enabled() {
                    println!(" signature: {:?}", signature);
                    println!(
                        " [{:width$}] {} confirmation failed: {:?}",
                        "blockrazor",
                        trade_type,
                        start_time.elapsed(),
                        width = crate::common::sdk_log::SWQOS_LABEL_WIDTH
                    );
                }
                return Err(e);
            }
        }
        if wait_confirmation && crate::common::sdk_log::sdk_log_enabled() {
            println!(" signature: {:?}", signature);
            println!(" [{:width$}] {} confirmed: {:?}", "blockrazor", trade_type, start_time.elapsed(), width = crate::common::sdk_log::SWQOS_LABEL_WIDTH);
        }

        Ok(())
    }
}

impl Drop for BlockRazorClient {
    fn drop(&mut self) {
        match &self.backend {
            BlockRazorBackend::Grpc { stop_ping, ping_handle, .. } | BlockRazorBackend::Http { stop_ping, ping_handle, .. } => {
                stop_ping.store(true, Ordering::Relaxed);

                let ping_handle = ping_handle.clone();
                tokio::spawn(async move {
                    let mut ping_guard = ping_handle.lock().await;
                    if let Some(handle) = ping_guard.as_ref() {
                        handle.abort();
                    }
                    *ping_guard = None;
                });
            }
        }
    }
}
