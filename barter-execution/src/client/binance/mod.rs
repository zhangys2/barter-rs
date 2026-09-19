use crate::{
    AccountEventKind, AccountSnapshot, InstrumentAccountSnapshot, UnindexedAccountEvent,
    UnindexedAccountSnapshot,
    balance::{AssetBalance, Balance},
    client::ExecutionClient,
    error::{ApiError, ClientError, ConnectivityError, UnindexedClientError, UnindexedOrderError},
    order::{
        Order, OrderEvent, OrderKey, OrderKind, TimeInForce,
        request::{OrderRequestCancel, OrderRequestOpen, UnindexedOrderResponseCancel},
        state::{Cancelled, Open, OrderState},
    },
    trade::Trade,
};
use barter_instrument::{
    Side,
    asset::{QuoteAsset, name::AssetNameExchange},
    exchange::ExchangeId,
    instrument::name::InstrumentNameExchange,
};
use barter_integration::{collection::snapshot::Snapshot, rate_limit::WeightWindow};
use chrono::{DateTime, Utc};
use fnv::FnvHashSet;
#[cfg(test)]
use futures::SinkExt;
use futures::{StreamExt, stream::BoxStream};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::VecDeque,
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::form_urlencoded;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct BinanceSpotConfig {
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    #[serde(default = "default_stream_base_url")]
    pub stream_base_url: String,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_api_secret_env")]
    pub api_secret_env: String,
    #[serde(default = "default_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_rate_limit_ms")]
    pub rate_limit_ms: u64,
    /// Conservative request-weight budget per minute (Binance IP limit is 6_000).
    #[serde(default = "default_weight_limit_per_minute")]
    pub weight_limit_per_minute: u32,
    /// Instruments used by [`ExecutionClient::fetch_trades`] when no prior open/snapshot seeded symbols.
    #[serde(default)]
    pub instruments: Vec<InstrumentNameExchange>,
}
fn default_api_base_url() -> String {
    "https://api.binance.com".into()
}
fn default_stream_base_url() -> String {
    "wss://stream.binance.com:9443".into()
}
fn default_api_key_env() -> String {
    "BINANCE_API_KEY".into()
}
fn default_api_secret_env() -> String {
    "BINANCE_API_SECRET".into()
}
fn default_recv_window_ms() -> u64 {
    5_000
}
fn default_rate_limit_ms() -> u64 {
    50
}
fn default_weight_limit_per_minute() -> u32 {
    1_200
}

impl Default for BinanceSpotConfig {
    fn default() -> Self {
        Self {
            api_base_url: default_api_base_url(),
            stream_base_url: default_stream_base_url(),
            api_key_env: default_api_key_env(),
            api_secret_env: default_api_secret_env(),
            recv_window_ms: default_recv_window_ms(),
            rate_limit_ms: default_rate_limit_ms(),
            weight_limit_per_minute: default_weight_limit_per_minute(),
            instruments: Vec::new(),
        }
    }
}

impl BinanceSpotConfig {
    /// Construct a Spot Testnet configuration. Credentials are still read only from the
    /// configured environment variable names.
    pub fn testnet() -> Self {
        Self {
            api_base_url: "https://testnet.binance.vision".into(),
            stream_base_url: "wss://stream.testnet.binance.vision".into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinanceSpot {
    config: BinanceSpotConfig,
    http: Client,
    limiter: std::sync::Arc<tokio::sync::Mutex<std::time::Instant>>,
    weights: std::sync::Arc<tokio::sync::Mutex<WeightWindow>>,
    symbols: std::sync::Arc<tokio::sync::Mutex<FnvHashSet<InstrumentNameExchange>>>,
    credentials_override: Option<(String, String)>,
}
impl BinanceSpot {
    fn credentials(&self) -> Result<(String, String), UnindexedClientError> {
        if let Some(credentials) = &self.credentials_override {
            return Ok(credentials.clone());
        }
        let key = env::var(&self.config.api_key_env)
            .map_err(|_| api_error("missing Binance API key environment variable"))?;
        let secret = env::var(&self.config.api_secret_env)
            .map_err(|_| api_error("missing Binance API secret environment variable"))?;
        if key.is_empty() || secret.is_empty() {
            return Err(api_error("Binance API credentials must not be empty"));
        }
        Ok((key, secret))
    }
    async fn track_symbol(&self, instrument: &InstrumentNameExchange) {
        self.symbols.lock().await.insert(instrument.clone());
    }

    async fn wait_rate_limit(&self) {
        let mut last = self.limiter.lock().await;
        let interval = Duration::from_millis(self.config.rate_limit_ms);
        let now = std::time::Instant::now();
        if let Some(wait) = interval.checked_sub(now.duration_since(*last)) {
            tokio::time::sleep(wait).await;
        }
        *last = std::time::Instant::now();
    }

    async fn wait_weight(&self, weight: u32) {
        let wait = {
            let mut window = self.weights.lock().await;
            window.reserve(weight, std::time::Instant::now())
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
            self.weights
                .lock()
                .await
                .start_new_window(weight, std::time::Instant::now());
        }
        self.wait_rate_limit().await;
    }

    fn observe_used_weight(&self, headers: &reqwest::header::HeaderMap) {
        let Some(value) = headers
            .get("x-mbx-used-weight-1m")
            .or_else(|| headers.get("X-MBX-USED-WEIGHT-1M"))
        else {
            return;
        };
        let Ok(text) = value.to_str() else {
            return;
        };
        let Ok(used) = text.parse::<u32>() else {
            return;
        };
        if let Ok(mut window) = self.weights.try_lock() {
            window.observe_used(used, std::time::Instant::now());
        }
    }

    async fn signed_get(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<Value, UnindexedClientError> {
        self.signed_request(reqwest::Method::GET, path, params)
            .await
    }

    /// Fetch Binance user trades for one symbol. The execution trait's historical-trades method
    /// lacks a symbol argument, so callers requiring history should use this explicit API.
    pub async fn fetch_trades_for_instrument(
        &self,
        instrument: &InstrumentNameExchange,
        time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        let value = self
            .signed_get(
                "/api/v3/myTrades",
                vec![
                    ("symbol", instrument.to_string()),
                    ("startTime", time_since.timestamp_millis().to_string()),
                    ("limit", "1000".into()),
                ],
            )
            .await?;
        let entries = value
            .as_array()
            .ok_or_else(|| api_error("Binance myTrades response was not an array"))?;
        let trades = entries
            .iter()
            .filter_map(|entry| {
                let id = entry.get("id")?.as_u64()?.to_string();
                let order_id = entry.get("orderId")?.as_u64()?.to_string();
                let price = decimal(entry.get("price")?.as_str()?).ok()?;
                let quantity = decimal(entry.get("qty")?.as_str()?).ok()?;
                let fee = decimal(entry.get("commission")?.as_str()?).ok()?;
                let time = DateTime::<Utc>::from_timestamp_millis(entry.get("time")?.as_i64()?)?;
                Some(Trade {
                    id: crate::trade::TradeId::new(id),
                    order_id: crate::order::id::OrderId::new(order_id),
                    instrument: instrument.clone(),
                    strategy: crate::order::id::StrategyId::new("binance"),
                    time_exchange: time,
                    side: if entry.get("isBuyer")?.as_bool()? {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    price,
                    quantity,
                    fees: crate::trade::AssetFees::quote_fees(fee),
                })
            })
            .collect::<Vec<_>>();
        Ok(trades)
    }

    async fn user_data_request(
        &self,
        method: reqwest::Method,
        params: Vec<(&str, String)>,
    ) -> Result<Value, UnindexedClientError> {
        self.wait_weight(1).await;
        let (api_key, _) = self.credentials()?;
        let response = self
            .http
            .request(
                method,
                format!("{}{}", self.config.api_base_url, "/api/v3/userDataStream"),
            )
            .header("X-MBX-APIKEY", api_key)
            .query(&params)
            .send()
            .await
            .map_err(connectivity_error)?;
        self.observe_used_weight(response.headers());
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|error| api_error(format!("invalid Binance user-data response: {error}")))?;
        if !status.is_success() {
            return Err(api_error(
                value
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("Binance user-data request failed"),
            ));
        }
        Ok(value)
    }

    async fn keepalive_listen_key(&self, listen_key: &str) {
        let _ = self
            .user_data_request(
                reqwest::Method::PUT,
                vec![("listenKey", listen_key.to_owned())],
            )
            .await;
    }

    async fn listen_key(&self) -> Result<String, UnindexedClientError> {
        let value = self
            .user_data_request(reqwest::Method::POST, vec![])
            .await?;
        value
            .get("listenKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| api_error("Binance user-data response omitted listenKey"))
    }
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        mut params: Vec<(&str, String)>,
    ) -> Result<Value, UnindexedClientError> {
        self.wait_weight(request_weight(&method, path)).await;
        let (api_key, secret) = self.credentials()?;
        params.push(("recvWindow", self.config.recv_window_ms.to_string()));
        params.push(("timestamp", now_millis().to_string()));
        let query = {
            let mut serializer = form_urlencoded::Serializer::new(String::new());
            for (key, value) in &params {
                serializer.append_pair(key, value);
            }
            serializer.finish()
        };
        let signature = sign_query(&query, &secret)?;
        let url = format!(
            "{}{}?{}&signature={}",
            self.config.api_base_url.trim_end_matches('/'),
            path,
            query,
            signature
        );
        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(connectivity_error)?;
        self.observe_used_weight(response.headers());
        let status = response.status();
        let body = response.text().await.map_err(connectivity_error)?;
        let value: Value = serde_json::from_str(&body)
            .map_err(|_| api_error(format!("Binance returned non-JSON response ({status})")))?;
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(UnindexedClientError::Api(ApiError::RateLimit));
        }
        if !status.is_success() {
            return Err(api_error(format!(
                "Binance API error: {}",
                value
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("request failed")
            )));
        }
        Ok(value)
    }
    fn order_from_response(value: &Value) -> Result<Open, UnindexedOrderError> {
        let id = value
            .get("orderId")
            .and_then(Value::as_u64)
            .map(|id| id.to_string())
            .ok_or_else(|| ApiError::OrderRejected("Binance response omitted orderId".into()))?;
        let filled_quantity = value
            .get("executedQty")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .parse()
            .map_err(|_| ApiError::OrderRejected("invalid Binance executedQty".into()))?;
        let time_millis = value
            .get("transactTime")
            .or_else(|| value.get("updateTime"))
            .and_then(Value::as_i64)
            .unwrap_or_else(now_millis);
        Ok(Open {
            id: crate::order::id::OrderId::new(id),
            time_exchange: DateTime::<Utc>::from_timestamp_millis(time_millis)
                .unwrap_or_else(Utc::now),
            filled_quantity,
        })
    }
}

#[cfg(test)]
fn test_client(config: BinanceSpotConfig) -> BinanceSpot {
    let symbols = config.instruments.iter().cloned().collect();
    let weights = WeightWindow::new(config.weight_limit_per_minute, Duration::from_secs(60));
    BinanceSpot {
        config,
        http: Client::new(),
        limiter: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::time::Instant::now() - Duration::from_secs(1),
        )),
        weights: std::sync::Arc::new(tokio::sync::Mutex::new(weights)),
        symbols: std::sync::Arc::new(tokio::sync::Mutex::new(symbols)),
        credentials_override: Some(("test-key".into(), "test-secret".into())),
    }
}

impl ExecutionClient for BinanceSpot {
    const EXCHANGE: ExchangeId = ExchangeId::BinanceSpot;
    type Config = BinanceSpotConfig;
    type AccountStream = BoxStream<'static, UnindexedAccountEvent>;
    fn new(config: Self::Config) -> Self {
        let symbols = config.instruments.iter().cloned().collect();
        Self {
            config: config.clone(),
            http: Client::new(),
            limiter: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::time::Instant::now() - Duration::from_secs(1),
            )),
            weights: std::sync::Arc::new(tokio::sync::Mutex::new(WeightWindow::new(
                config.weight_limit_per_minute,
                Duration::from_secs(60),
            ))),
            symbols: std::sync::Arc::new(tokio::sync::Mutex::new(symbols)),
            credentials_override: None,
        }
    }
    async fn account_snapshot(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<UnindexedAccountSnapshot, UnindexedClientError> {
        let value = self.signed_get("/api/v3/account", vec![]).await?;
        let balances = value
            .get("balances")
            .and_then(Value::as_array)
            .ok_or_else(|| api_error("Binance account response omitted balances"))?
            .iter()
            .filter_map(|item| {
                let asset = AssetNameExchange::from(item.get("asset")?.as_str()?.to_owned());
                if !assets.is_empty() && !assets.contains(&asset) {
                    return None;
                }
                let free = decimal(item.get("free")?.as_str()?).ok()?;
                let locked = decimal(item.get("locked")?.as_str()?).ok()?;
                Some(AssetBalance {
                    asset,
                    balance: Balance {
                        total: free + locked,
                        free,
                    },
                    time_exchange: Utc::now(),
                })
            })
            .collect();
        for instrument in instruments {
            self.track_symbol(instrument).await;
        }
        let mut snapshots = Vec::new();
        for instrument in instruments {
            let orders = self
                .fetch_open_orders(std::slice::from_ref(instrument))
                .await?;
            snapshots.push(InstrumentAccountSnapshot {
                instrument: instrument.clone(),
                orders: orders.into_iter().map(|order| order.into()).collect(),
            });
        }
        Ok(AccountSnapshot {
            exchange: Self::EXCHANGE,
            balances,
            instruments: snapshots,
        })
    }
    async fn account_stream(
        &self,
        assets: &[AssetNameExchange],
        instruments: &[InstrumentNameExchange],
    ) -> Result<Self::AccountStream, UnindexedClientError> {
        let client = self.clone();
        let assets = assets.to_vec();
        let instruments = instruments.to_vec();
        for instrument in &instruments {
            client.track_symbol(instrument).await;
        }
        type BinanceWebSocket = tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >;
        Ok(Box::pin(futures::stream::unfold(
            (
                client,
                assets,
                instruments,
                None::<BinanceWebSocket>,
                None::<String>,
                std::time::Instant::now(),
                VecDeque::new(),
            ),
            |(
                client,
                assets,
                instruments,
                mut socket,
                mut listen_key,
                mut last_keepalive,
                mut pending,
            )| async move {
                loop {
                    if let Some(event) = pending.pop_front() {
                        return Some((
                            event,
                            (
                                client,
                                assets,
                                instruments,
                                socket,
                                listen_key,
                                last_keepalive,
                                pending,
                            ),
                        ));
                    }
                    if socket.is_none() {
                        let Ok(new_listen_key) = client.listen_key().await else {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        };
                        let ws_url =
                            format!("{}/ws/{new_listen_key}", client.config.stream_base_url);
                        match connect_async(ws_url).await {
                            Ok((connected, _)) => {
                                listen_key = Some(new_listen_key);
                                socket = Some(connected);
                                last_keepalive = std::time::Instant::now();
                                if let Ok(snapshot) =
                                    client.account_snapshot(&assets, &instruments).await
                                {
                                    return Some((
                                        UnindexedAccountEvent {
                                            exchange: ExchangeId::BinanceSpot,
                                            kind: AccountEventKind::Snapshot(snapshot),
                                        },
                                        (
                                            client,
                                            assets,
                                            instruments,
                                            socket,
                                            listen_key,
                                            last_keepalive,
                                            pending,
                                        ),
                                    ));
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue;
                            }
                        }
                    }
                    if last_keepalive.elapsed() >= Duration::from_secs(30 * 60) {
                        if let Some(key) = listen_key.as_deref() {
                            client.keepalive_listen_key(key).await;
                        }
                        last_keepalive = std::time::Instant::now();
                    }
                    let Some(ws) = socket.as_mut() else { continue };
                    match ws.next().await {
                        Some(Ok(message)) => {
                            if let Message::Text(text) = message
                                && let Ok(value) = serde_json::from_str::<Value>(&text)
                            {
                                if let Some(event) = parse_user_data_trade(&value) {
                                    pending.push_back(event);
                                }
                                if let Some(event) = parse_user_data_order(&value) {
                                    pending.push_back(event);
                                }
                                if let Some(event) = pending.pop_front() {
                                    return Some((
                                        event,
                                        (
                                            client,
                                            assets,
                                            instruments,
                                            socket,
                                            listen_key,
                                            last_keepalive,
                                            pending,
                                        ),
                                    ));
                                }
                            }
                            // Balance updates and unrecognised execution messages are reconciled
                            // through a fresh REST snapshot, avoiding partial state after drops.
                            if let Ok(snapshot) =
                                client.account_snapshot(&assets, &instruments).await
                            {
                                return Some((
                                    UnindexedAccountEvent {
                                        exchange: ExchangeId::BinanceSpot,
                                        kind: AccountEventKind::Snapshot(snapshot),
                                    },
                                    (
                                        client,
                                        assets,
                                        instruments,
                                        socket,
                                        listen_key,
                                        last_keepalive,
                                        pending,
                                    ),
                                ));
                            }
                        }
                        Some(Err(_)) | None => {
                            socket = None;
                            listen_key = None;
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            },
        )))
    }
    async fn cancel_order(
        &self,
        request: OrderRequestCancel<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<UnindexedOrderResponseCancel> {
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };
        let mut params = vec![
            ("symbol", request.key.instrument.to_string()),
            ("origClientOrderId", request.key.cid.to_string()),
        ];
        if let Some(id) = &request.state.id {
            params.push(("orderId", id.to_string()));
        }
        let state = match self
            .signed_request(reqwest::Method::DELETE, "/api/v3/order", params)
            .await
        {
            Ok(value) => value
                .get("orderId")
                .and_then(Value::as_u64)
                .map(|id| {
                    Ok(Cancelled {
                        id: crate::order::id::OrderId::new(id.to_string()),
                        time_exchange: Utc::now(),
                    })
                })
                .unwrap_or_else(|| Err(api_order_error("Binance cancel response omitted orderId"))),
            Err(error) => Err(order_error(error)),
        };
        Some(OrderEvent { key, state })
    }
    async fn open_order(
        &self,
        request: OrderRequestOpen<ExchangeId, &InstrumentNameExchange>,
    ) -> Option<Order<ExchangeId, InstrumentNameExchange, Result<Open, UnindexedOrderError>>> {
        let key = OrderKey {
            exchange: request.key.exchange,
            instrument: request.key.instrument.clone(),
            strategy: request.key.strategy.clone(),
            cid: request.key.cid.clone(),
        };
        self.track_symbol(request.key.instrument).await;
        let mut params = vec![
            ("symbol", request.key.instrument.to_string()),
            (
                "side",
                match request.state.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                }
                .into(),
            ),
            (
                "type",
                match request.state.kind {
                    OrderKind::Market => "MARKET",
                    OrderKind::Limit => "LIMIT",
                }
                .into(),
            ),
            ("quantity", request.state.quantity.abs().to_string()),
            ("newClientOrderId", request.key.cid.to_string()),
        ];
        if request.state.kind == OrderKind::Limit {
            params.push(("price", request.state.price.to_string()));
            params.push(("timeInForce", tif(request.state.time_in_force).into()));
        }
        let state = self
            .signed_request(reqwest::Method::POST, "/api/v3/order", params)
            .await
            .map_err(order_error)
            .and_then(|value| Self::order_from_response(&value));
        Some(Order {
            key,
            side: request.state.side,
            price: request.state.price,
            quantity: request.state.quantity,
            kind: request.state.kind,
            time_in_force: request.state.time_in_force,
            state,
        })
    }
    async fn fetch_balances(
        &self,
        assets: &[AssetNameExchange],
    ) -> Result<Vec<AssetBalance<AssetNameExchange>>, UnindexedClientError> {
        Ok(self.account_snapshot(assets, &[]).await?.balances)
    }
    async fn fetch_open_orders(
        &self,
        instruments: &[InstrumentNameExchange],
    ) -> Result<Vec<Order<ExchangeId, InstrumentNameExchange, Open>>, UnindexedClientError> {
        let mut result = Vec::new();
        for instrument in instruments {
            self.symbols.lock().await.insert(instrument.clone());
            let value = self
                .signed_get(
                    "/api/v3/openOrders",
                    vec![("symbol", instrument.to_string())],
                )
                .await?;
            for order in value
                .as_array()
                .ok_or_else(|| api_error("Binance openOrders response was not an array"))?
            {
                if let Some(mapped) = parse_open_order(instrument, order) {
                    result.push(mapped);
                }
            }
        }
        Ok(result)
    }
    async fn fetch_trades(
        &self,
        time_since: DateTime<Utc>,
    ) -> Result<Vec<Trade<QuoteAsset, InstrumentNameExchange>>, UnindexedClientError> {
        let symbols: Vec<_> = self.symbols.lock().await.iter().cloned().collect();
        let mut trades = Vec::new();
        for symbol in symbols {
            trades.extend(
                self.fetch_trades_for_instrument(&symbol, time_since)
                    .await?,
            );
        }
        Ok(trades)
    }
}

fn parse_open_order(
    instrument: &InstrumentNameExchange,
    value: &Value,
) -> Option<Order<ExchangeId, InstrumentNameExchange, Open>> {
    let id = value.get("orderId")?.as_u64()?.to_string();
    let side = if value.get("side")?.as_str()? == "BUY" {
        Side::Buy
    } else {
        Side::Sell
    };
    let price = decimal(value.get("price")?.as_str()?).ok()?;
    let quantity = decimal(value.get("origQty")?.as_str()?).ok()?;
    let filled_quantity = decimal(value.get("executedQty")?.as_str()?).ok()?;
    let time_exchange = DateTime::<Utc>::from_timestamp_millis(value.get("time")?.as_i64()?)?;
    Some(Order {
        key: OrderKey {
            exchange: ExchangeId::BinanceSpot,
            instrument: instrument.clone(),
            strategy: crate::order::id::StrategyId::new("binance"),
            cid: crate::order::id::ClientOrderId::new(value.get("clientOrderId")?.as_str()?),
        },
        side,
        price,
        quantity,
        kind: if value.get("type")?.as_str()? == "LIMIT" {
            OrderKind::Limit
        } else {
            OrderKind::Market
        },
        time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
        state: Open {
            id: crate::order::id::OrderId::new(id),
            time_exchange,
            filled_quantity,
        },
    })
}
fn tif(value: TimeInForce) -> &'static str {
    match value {
        TimeInForce::FillOrKill => "FOK",
        TimeInForce::ImmediateOrCancel => "IOC",
        _ => "GTC",
    }
}
fn decimal(value: &str) -> Result<rust_decimal::Decimal, ()> {
    value.parse().map_err(|_| ())
}
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
fn sign_query(query: &str, secret: &str) -> Result<String, UnindexedClientError> {
    let mut signer = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| api_error("invalid Binance signing key"))?;
    signer.update(query.as_bytes());
    Ok(hex::encode(signer.finalize().into_bytes()))
}
fn api_error(message: impl Into<String>) -> UnindexedClientError {
    UnindexedClientError::Api(ApiError::OrderRejected(message.into()))
}
fn order_error(error: UnindexedClientError) -> UnindexedOrderError {
    match error {
        ClientError::Connectivity(error) => error.into(),
        ClientError::Api(error) => error.into(),
        other => api_order_error(other.to_string()),
    }
}
fn parse_user_data_trade(value: &Value) -> Option<UnindexedAccountEvent> {
    if value.get("e")?.as_str()? != "executionReport" {
        return None;
    }
    let quantity = decimal(value.get("l")?.as_str()?).ok()?;
    if quantity.is_zero() {
        return None;
    }
    let price = decimal(value.get("L")?.as_str()?).ok()?;
    let fee = decimal(value.get("n")?.as_str()?).ok()?;
    let millis = value
        .get("T")
        .and_then(Value::as_i64)
        .unwrap_or_else(now_millis);
    Some(UnindexedAccountEvent {
        exchange: ExchangeId::BinanceSpot,
        kind: AccountEventKind::Trade(crate::trade::Trade {
            id: crate::trade::TradeId::new(value.get("t")?.as_u64()?.to_string()),
            order_id: crate::order::id::OrderId::new(value.get("i")?.as_u64()?.to_string()),
            instrument: InstrumentNameExchange::from(value.get("s")?.as_str()?.to_owned()),
            strategy: crate::order::id::StrategyId::new("binance"),
            time_exchange: DateTime::<Utc>::from_timestamp_millis(millis)?,
            side: if value.get("S")?.as_str()? == "BUY" {
                Side::Buy
            } else {
                Side::Sell
            },
            price,
            quantity,
            fees: crate::trade::AssetFees::quote_fees(fee),
        }),
    })
}

fn parse_user_data_order(value: &Value) -> Option<UnindexedAccountEvent> {
    if value.get("e")?.as_str()? != "executionReport" {
        return None;
    }
    let symbol = InstrumentNameExchange::from(value.get("s")?.as_str()?.to_owned());
    let id = value.get("orderId")?.as_u64()?.to_string();
    let cid = value
        .get("c")
        .and_then(Value::as_str)
        .map(crate::order::id::ClientOrderId::new)?;
    let side = match value.get("S")?.as_str()? {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return None,
    };
    let price = decimal(value.get("p")?.as_str()?).ok()?;
    let quantity = decimal(value.get("q")?.as_str()?).ok()?;
    let executed = decimal(value.get("z")?.as_str()?).ok()?;
    let millis = value
        .get("T")
        .and_then(Value::as_i64)
        .unwrap_or_else(now_millis);
    let time_exchange = DateTime::<Utc>::from_timestamp_millis(millis).unwrap_or_else(Utc::now);
    let state = match value.get("X").and_then(Value::as_str).unwrap_or("NEW") {
        "CANCELED" | "EXPIRED" | "REJECTED" => OrderState::inactive(Cancelled {
            id: crate::order::id::OrderId::new(id.clone()),
            time_exchange,
        }),
        "FILLED" => OrderState::fully_filled(),
        _ => OrderState::active(Open {
            id: crate::order::id::OrderId::new(id),
            time_exchange,
            filled_quantity: executed,
        }),
    };
    Some(UnindexedAccountEvent {
        exchange: ExchangeId::BinanceSpot,
        kind: AccountEventKind::OrderSnapshot(Snapshot(Order {
            key: OrderKey {
                exchange: ExchangeId::BinanceSpot,
                instrument: symbol,
                strategy: crate::order::id::StrategyId::new("binance"),
                cid,
            },
            side,
            price,
            quantity,
            kind: if value.get("o").and_then(Value::as_str) == Some("MARKET") {
                OrderKind::Market
            } else {
                OrderKind::Limit
            },
            time_in_force: TimeInForce::GoodUntilCancelled { post_only: false },
            state,
        })),
    })
}

fn request_weight(method: &reqwest::Method, path: &str) -> u32 {
    match (method, path) {
        (&reqwest::Method::GET, "/api/v3/account") => 20,
        (&reqwest::Method::GET, "/api/v3/myTrades") => 20,
        (&reqwest::Method::GET, "/api/v3/openOrders") => 6,
        (&reqwest::Method::POST, "/api/v3/order") => 1,
        (&reqwest::Method::DELETE, "/api/v3/order") => 1,
        _ => 1,
    }
}

fn api_order_error(message: impl Into<String>) -> UnindexedOrderError {
    UnindexedOrderError::Rejected(ApiError::OrderRejected(message.into()))
}
fn connectivity_error(error: reqwest::Error) -> UnindexedClientError {
    UnindexedClientError::Connectivity(ConnectivityError::Socket(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_tests_enabled() -> bool {
        std::env::var("BARTER_LIVE_TESTS").as_deref() == Ok("1")
            && std::env::var("BINANCE_API_KEY")
                .ok()
                .filter(|value| !value.is_empty())
                .is_some()
            && std::env::var("BINANCE_API_SECRET")
                .ok()
                .filter(|value| !value.is_empty())
                .is_some()
    }

    #[test]
    fn config_defaults_reference_environment_only() {
        let config: BinanceSpotConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.api_base_url, "https://api.binance.com");
        assert_eq!(config.stream_base_url, "wss://stream.binance.com:9443");
        assert_eq!(config.api_key_env, "BINANCE_API_KEY");
        assert_eq!(config.api_secret_env, "BINANCE_API_SECRET");
        assert_eq!(config.recv_window_ms, 5_000);
        assert_eq!(config.rate_limit_ms, 50);
        assert_eq!(config.weight_limit_per_minute, 1_200);
        assert!(config.instruments.is_empty());
    }

    #[tokio::test]
    async fn mock_websocket_user_data_event_is_consumed() {
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_address = http.local_addr().unwrap();
        let http_task = tokio::spawn(async move {
            for body in [r#"{"listenKey":"test-key"}"#, r#"{"balances":[]}"#] {
                let (mut socket, _) = http.accept().await.unwrap();
                let mut request = vec![0; 2048];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut request)
                    .await
                    .unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_address = ws_listener.local_addr().unwrap();
        let ws_task = tokio::spawn(async move {
            let (socket, _) = ws_listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            socket.send(Message::Text(r#"{"e":"executionReport","s":"BTCUSDT","orderId":7,"c":"client-7","S":"BUY","p":"100","q":"2","z":"1","T":1,"X":"PARTIALLY_FILLED","o":"LIMIT"}"#.into())).await.unwrap();
        });
        let config = BinanceSpotConfig {
            api_base_url: format!("http://{http_address}"),
            stream_base_url: format!("ws://{ws_address}"),
            ..Default::default()
        };
        let client = test_client(config);
        let mut stream = client.account_stream(&[], &[]).await.unwrap();
        assert!(matches!(
            stream.next().await.unwrap().kind,
            AccountEventKind::Snapshot(_)
        ));
        let event = stream.next().await.unwrap();
        assert!(matches!(event.kind, AccountEventKind::OrderSnapshot(_)));
        http_task.await.unwrap();
        ws_task.await.unwrap();
    }

    #[tokio::test]
    async fn mock_http_my_trades_round_trip_is_signed_and_mapped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let size = tokio::io::AsyncReadExt::read(&mut socket, &mut request)
                .await
                .unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.starts_with("GET /api/v3/myTrades?"));
            assert!(request.contains("signature="));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-mbx-apikey: test-key")
            );
            let body = r#"[{"id":8,"orderId":7,"price":"100","qty":"1","commission":"0.1","time":1,"isBuyer":true}]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes())
                .await
                .unwrap();
        });
        let config = BinanceSpotConfig {
            api_base_url: format!("http://{address}"),
            stream_base_url: "wss://stream.binance.com:9443".into(),
            api_key_env: "BARTER_TEST_BINANCE_KEY".into(),
            api_secret_env: "BARTER_TEST_BINANCE_SECRET".into(),
            recv_window_ms: 5_000,
            rate_limit_ms: 0,
            weight_limit_per_minute: 6_000,
            instruments: vec![InstrumentNameExchange::from("BTCUSDT")],
        };
        let client = test_client(config);
        let trades = client
            .fetch_trades_for_instrument(
                &InstrumentNameExchange::from("BTCUSDT"),
                DateTime::<Utc>::UNIX_EPOCH,
            )
            .await
            .unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].quantity, rust_decimal::Decimal::ONE);
        server.await.unwrap();
    }

    #[test]
    fn execution_report_fill_is_translated_to_trade() {
        let value: Value = serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "i": 7, "t": 8,
            "S": "BUY", "L": "100", "l": "1", "n": "0.1", "T": 1
        });
        let event = parse_user_data_trade(&value).unwrap();
        assert!(matches!(event.kind, AccountEventKind::Trade(_)));
    }

    #[test]
    fn execution_report_is_translated_to_order_snapshot() {
        let value: Value = serde_json::json!({
            "e": "executionReport", "s": "BTCUSDT", "orderId": 7,
            "c": "client-7", "S": "BUY", "p": "100", "q": "2", "z": "1",
            "T": 1, "X": "PARTIALLY_FILLED", "o": "LIMIT"
        });
        let event = parse_user_data_order(&value).unwrap();
        assert!(matches!(event.kind, AccountEventKind::OrderSnapshot(_)));
    }

    #[test]
    fn testnet_config_changes_endpoint_without_embedding_credentials() {
        let config = BinanceSpotConfig::testnet();
        assert_eq!(config.api_base_url, "https://testnet.binance.vision");
        assert_eq!(
            config.stream_base_url,
            "wss://stream.testnet.binance.vision"
        );
        assert_eq!(config.api_key_env, "BINANCE_API_KEY");
        assert_eq!(config.api_secret_env, "BINANCE_API_SECRET");
    }

    #[tokio::test]
    #[ignore = "requires BINANCE_API_KEY/BINANCE_API_SECRET and explicit BARTER_LIVE_TESTS=1"]
    async fn gated_testnet_account_snapshot() {
        if !live_tests_enabled() {
            return;
        }
        let client = BinanceSpot::new(BinanceSpotConfig::testnet());
        client.account_snapshot(&[], &[]).await.unwrap();
    }

    #[tokio::test]
    async fn fetch_trades_uses_configured_instruments_without_side_api() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let size = tokio::io::AsyncReadExt::read(&mut socket, &mut request)
                .await
                .unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
            assert!(request.contains("/api/v3/myTrades?"));
            assert!(request.contains("symbol=BTCUSDT"));
            let body = r#"[{"id":8,"orderId":7,"price":"100","qty":"1","commission":"0.1","time":1,"isBuyer":true}]"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nx-mbx-used-weight-1m: 20\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes())
                .await
                .unwrap();
        });
        let config = BinanceSpotConfig {
            api_base_url: format!("http://{address}"),
            rate_limit_ms: 0,
            weight_limit_per_minute: 6_000,
            instruments: vec![InstrumentNameExchange::from("BTCUSDT")],
            ..Default::default()
        };
        let client = test_client(config);
        let trades = ExecutionClient::fetch_trades(&client, DateTime::<Utc>::UNIX_EPOCH)
            .await
            .unwrap();
        assert_eq!(trades.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn weight_window_spaces_a_burst_so_the_server_does_not_429() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut last: Option<std::time::Instant> = None;
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let now = std::time::Instant::now();
                if let Some(previous) = last {
                    assert!(
                        now.duration_since(previous) >= std::time::Duration::from_millis(20),
                        "burst was not spaced by the weight window"
                    );
                }
                last = Some(now);
                let mut request = vec![0; 4096];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut request)
                    .await
                    .unwrap();
                let body = r#"{"balances":[]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nx-mbx-used-weight-1m: 20\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                tokio::io::AsyncWriteExt::write_all(&mut socket, response.as_bytes())
                    .await
                    .unwrap();
            }
            let _ = tx.send(());
        });
        let config = BinanceSpotConfig {
            api_base_url: format!("http://{address}"),
            rate_limit_ms: 0,
            weight_limit_per_minute: 20,
            ..Default::default()
        };
        let client = test_client(config);
        // Shrink the window so the test is fast: replace the client's window.
        *client.weights.lock().await = WeightWindow::new(20, Duration::from_millis(40));
        client.account_snapshot(&[], &[]).await.unwrap();
        client.account_snapshot(&[], &[]).await.unwrap();
        rx.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    #[ignore = "requires BINANCE_API_KEY/BINANCE_API_SECRET and explicit BARTER_LIVE_TESTS=1"]
    async fn gated_testnet_open_cancel_reconcile() {
        if !live_tests_enabled() {
            return;
        }
        let mut config = BinanceSpotConfig::testnet();
        let instrument = InstrumentNameExchange::from("BTCUSDT");
        config.instruments = vec![instrument.clone()];
        let client = BinanceSpot::new(config);
        let snapshot = client
            .account_snapshot(&[], std::slice::from_ref(&instrument))
            .await
            .unwrap();
        let cid = crate::order::id::ClientOrderId::random();
        let open = client
            .open_order(OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: crate::order::id::StrategyId::new("live-round-trip"),
                    cid: cid.clone(),
                },
                state: crate::order::request::RequestOpen {
                    side: Side::Buy,
                    price: rust_decimal::Decimal::from(1000),
                    quantity: rust_decimal::Decimal::new(1, 5),
                    kind: OrderKind::Limit,
                    time_in_force: TimeInForce::GoodUntilCancelled { post_only: true },
                },
            })
            .await
            .expect("open response");
        let opened = open.state.expect("testnet accepted far limit");
        let opens = client
            .fetch_open_orders(std::slice::from_ref(&instrument))
            .await
            .unwrap();
        assert!(
            opens.iter().any(|order| order.key.cid == cid),
            "venue open orders must include the Engine-bound cid"
        );
        let cancelled = client
            .cancel_order(OrderEvent {
                key: OrderKey {
                    exchange: ExchangeId::BinanceSpot,
                    instrument: &instrument,
                    strategy: crate::order::id::StrategyId::new("live-round-trip"),
                    cid: cid.clone(),
                },
                state: crate::order::request::RequestCancel {
                    id: Some(opened.id.clone()),
                },
            })
            .await
            .expect("cancel response");
        assert!(cancelled.state.is_ok());
        let opens = client
            .fetch_open_orders(&[InstrumentNameExchange::from("BTCUSDT")])
            .await
            .unwrap();
        assert!(opens.iter().all(|order| order.key.cid != cid));
        let trades = ExecutionClient::fetch_trades(&client, DateTime::<Utc>::UNIX_EPOCH)
            .await
            .unwrap();
        let _ = (snapshot, trades);
    }

    #[test]
    fn signing_is_deterministic_and_sha256_length() {
        let first = sign_query("symbol=BTCUSDT&timestamp=1", "secret").unwrap();
        let second = sign_query("symbol=BTCUSDT&timestamp=1", "secret").unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
    }
}
