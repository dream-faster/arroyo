use anyhow::{Context as _, anyhow, bail};
use arroyo_rpc::native_cert_store;
use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token;
use chrono::TimeZone;
use futures::{StreamExt, future::BoxFuture};
use rskafka::client::{
    Client, ClientBuilder, Credentials, OauthBearerCredentials, OauthCallback, SaslConfig,
    consumer::{StartOffset, StreamConsumerBuilder},
    partition::{Compression, PartitionClient, UnknownTopicHandling},
};
use rskafka::record::{Record, RecordAndOffset};
use rustls023::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::{Context, KafkaConfigAuthentication, SourceOffset};

pub(crate) const MAX_FETCH_WAIT_MS: i32 = 500;
#[cfg(test)]
const TOPIC_ADMIN_TIMEOUT_MS: i32 = 5_000;

#[derive(Debug)]
pub(crate) struct TopicMetadata {
    pub(crate) partitions: Vec<i32>,
}

#[derive(Debug)]
pub(crate) struct ConsumedRecord {
    pub(crate) partition: i32,
    pub(crate) record: RecordAndOffset,
}

#[derive(Clone, Debug)]
struct ResolvedClientConfig {
    client_id: Option<String>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    tls_config: Option<Arc<ClientConfig>>,
    sasl_config: Option<SaslConfig>,
}

pub(crate) async fn build_client(
    bootstrap_servers: &str,
    client_configs: &HashMap<String, String>,
    context: &Context,
) -> anyhow::Result<Client> {
    let resolved = ResolvedClientConfig::from_map(client_configs, context)?;
    let mut builder = ClientBuilder::new(
        bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
    )
    .connect_timeout(resolved.connect_timeout)
    .timeout(resolved.request_timeout);

    if let Some(client_id) = resolved.client_id {
        builder = builder.client_id(client_id);
    }

    if let Some(tls_config) = resolved.tls_config {
        builder = builder.tls_config(tls_config);
    }

    if let Some(sasl_config) = resolved.sasl_config {
        builder = builder.sasl_config(sasl_config);
    }

    builder
        .build()
        .await
        .map_err(|e| anyhow!("failed to connect to Kafka: {e}"))
}

pub(crate) async fn topic_metadata(
    client: &Client,
    topic: &str,
) -> anyhow::Result<Option<TopicMetadata>> {
    let topic = client
        .list_topics()
        .await
        .context("failed to fetch Kafka topic metadata")?
        .into_iter()
        .find(|t| t.name == topic);

    Ok(topic.map(|t| TopicMetadata {
        partitions: t.partitions.into_iter().collect(),
    }))
}

pub(crate) async fn fetch_topics(client: &Client) -> anyhow::Result<Vec<(String, usize)>> {
    Ok(client
        .list_topics()
        .await
        .context("failed to list Kafka topics")?
        .into_iter()
        .map(|topic| (topic.name, topic.partitions.len()))
        .collect())
}

#[cfg(test)]
pub(crate) async fn create_topic(
    client: &Client,
    topic: &str,
    partitions: i32,
) -> anyhow::Result<()> {
    client
        .controller_client()
        .context("failed to create Kafka controller client")?
        .create_topic(topic.to_string(), partitions, 1, TOPIC_ADMIN_TIMEOUT_MS)
        .await
        .map_err(|e| anyhow!("failed to create Kafka topic '{topic}': {e}"))?;

    for _ in 0..50 {
        if topic_metadata(client, topic).await?.is_some() {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    bail!("Kafka topic '{topic}' was created but did not become visible in metadata")
}

#[cfg(test)]
pub(crate) async fn delete_topic(client: &Client, topic: &str) -> anyhow::Result<()> {
    client
        .controller_client()
        .context("failed to create Kafka controller client")?
        .delete_topic(topic.to_string(), TOPIC_ADMIN_TIMEOUT_MS)
        .await
        .map_err(|e| anyhow!("failed to delete Kafka topic '{topic}': {e}"))
}

pub(crate) async fn partition_client(
    client: &Client,
    topic: &str,
    partition: i32,
) -> anyhow::Result<Arc<PartitionClient>> {
    Ok(Arc::new(
        client
            .partition_client(topic.to_string(), partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| {
                anyhow!("failed to connect to Kafka partition {topic}[{partition}]: {e}")
            })?,
    ))
}

pub(crate) fn spawn_partition_consumers(
    topic: &str,
    partitions: Vec<(i32, Arc<PartitionClient>, StartOffset)>,
) -> (
    mpsc::Receiver<anyhow::Result<ConsumedRecord>>,
    Vec<JoinHandle<()>>,
) {
    let (tx, rx) = mpsc::channel(128);
    let topic = topic.to_string();
    let handles = partitions
        .into_iter()
        .map(|(partition, client, start_offset)| {
            let tx = tx.clone();
            let topic = topic.clone();
            tokio::spawn(async move {
                let mut stream = StreamConsumerBuilder::new(client, start_offset)
                    .with_max_wait_ms(MAX_FETCH_WAIT_MS)
                    .build();

                while let Some(result) = stream.next().await {
                    let result = result
                        .map(|(record, _)| ConsumedRecord { partition, record })
                        .map_err(|e| {
                            anyhow!("failed to read from Kafka topic {topic}[{partition}]: {e}")
                        });
                    if tx.send(result).await.is_err() {
                        break;
                    }
                }
            })
        })
        .collect();
    drop(tx);
    (rx, handles)
}

pub(crate) fn start_offset(
    offset: SourceOffset,
    restored_offset: Option<i64>,
) -> anyhow::Result<StartOffset> {
    if let Some(offset) = restored_offset {
        return Ok(StartOffset::At(offset));
    }

    match offset {
        SourceOffset::Earliest => Ok(StartOffset::Earliest),
        SourceOffset::Latest => Ok(StartOffset::Latest),
        SourceOffset::Group => bail!(
            "Kafka sources configured with source.offset='group' are not supported by the rskafka client"
        ),
    }
}

pub(crate) fn kafka_record(key: Option<Vec<u8>>, value: Vec<u8>, timestamp_millis: i64) -> Record {
    Record {
        key,
        value: Some(value),
        headers: Default::default(),
        timestamp: chrono::Utc
            .timestamp_millis_opt(timestamp_millis)
            .single()
            .unwrap_or_else(|| chrono::Utc.timestamp_millis_opt(0).unwrap()),
    }
}

pub(crate) async fn produce_records(
    client: &PartitionClient,
    records: Vec<Record>,
) -> anyhow::Result<Vec<i64>> {
    client
        .produce(records, Compression::NoCompression)
        .await
        .map(|result| result.offsets)
        .map_err(|e| anyhow!("failed to write records to Kafka: {e}"))
}

impl Context {
    fn oauth_callback(&self) -> anyhow::Result<OauthCallback> {
        let Some(KafkaConfigAuthentication::AwsMskIam { region }) =
            self.config.as_ref().map(|c| &c.authentication)
        else {
            bail!("only AWS_MSK_IAM is supported for sasl oauth");
        };

        let region = region.clone();
        Ok(Arc::new(move || {
            let region = region.clone();
            Box::pin(async move {
                let (token, _) = tokio::time::timeout(
                    Duration::from_secs(10),
                    generate_auth_token(Region::new(region.clone())),
                )
                .await
                .map_err(|e| anyhow!("timed out generating MSK oauth token: {e:?}"))?
                .map_err(|e| anyhow!("failed to sign MSK IAM auth token: {e:?}"))?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(token)
            })
                as BoxFuture<
                    'static,
                    std::result::Result<String, Box<dyn std::error::Error + Send + Sync>>,
                >
        }))
    }
}

impl ResolvedClientConfig {
    fn from_map(
        client_configs: &HashMap<String, String>,
        context: &Context,
    ) -> anyhow::Result<Self> {
        let mut configs = client_configs.clone();

        let client_id = configs.remove("client.id");
        let connect_timeout = configs
            .remove("socket.connection.setup.timeout.ms")
            .map(|timeout| parse_timeout_ms("socket.connection.setup.timeout.ms", &timeout))
            .transpose()?
            .flatten();
        let request_timeout = configs
            .remove("request.timeout.ms")
            .map(|timeout| parse_timeout_ms("request.timeout.ms", &timeout))
            .transpose()?
            .flatten();

        let security_protocol = configs
            .remove("security.protocol")
            .unwrap_or_else(|| "PLAINTEXT".to_string())
            .to_ascii_uppercase();
        let mechanism = configs
            .remove("sasl.mechanism")
            .map(|m| m.to_ascii_uppercase());
        let username = configs.remove("sasl.username");
        let password = configs.remove("sasl.password");

        let sasl_required = matches!(security_protocol.as_str(), "SASL_PLAINTEXT" | "SASL_SSL");
        let tls_config = match security_protocol.as_str() {
            "PLAINTEXT" | "SASL_PLAINTEXT" => None,
            "SSL" | "SASL_SSL" => Some(Arc::new(
                ClientConfig::builder()
                    .with_root_certificates((*native_cert_store()).clone())
                    .with_no_client_auth(),
            )),
            other => bail!("unsupported Kafka security.protocol '{other}'"),
        };

        let sasl_config = if sasl_required {
            let mechanism = mechanism.ok_or_else(|| anyhow!("missing sasl.mechanism"))?;
            Some(match mechanism.as_str() {
                "PLAIN" => SaslConfig::Plain(Credentials::new(
                    username.ok_or_else(|| anyhow!("missing sasl.username"))?,
                    password.ok_or_else(|| anyhow!("missing sasl.password"))?,
                )),
                "SCRAM-SHA-256" => SaslConfig::ScramSha256(Credentials::new(
                    username.ok_or_else(|| anyhow!("missing sasl.username"))?,
                    password.ok_or_else(|| anyhow!("missing sasl.password"))?,
                )),
                "SCRAM-SHA-512" => SaslConfig::ScramSha512(Credentials::new(
                    username.ok_or_else(|| anyhow!("missing sasl.username"))?,
                    password.ok_or_else(|| anyhow!("missing sasl.password"))?,
                )),
                "OAUTHBEARER" => SaslConfig::Oauthbearer(OauthBearerCredentials {
                    callback: context.oauth_callback()?,
                    authz_id: None,
                    bearer_kvs: vec![],
                }),
                other => bail!("unsupported Kafka sasl.mechanism '{other}'"),
            })
        } else {
            if let Some(mechanism) = mechanism {
                bail!(
                    "sasl.mechanism is only supported with SASL_PLAINTEXT or SASL_SSL, found '{mechanism}' with '{security_protocol}'"
                );
            }
            if username.is_some() || password.is_some() {
                bail!(
                    "sasl.username and sasl.password are only supported with SASL_PLAINTEXT or SASL_SSL"
                );
            }
            None
        };

        if !configs.is_empty() {
            let mut unsupported: Vec<_> = configs.into_keys().collect();
            unsupported.sort();
            bail!(
                "unsupported Kafka client configs for rskafka migration: {}",
                unsupported.join(", ")
            );
        }

        Ok(Self {
            client_id,
            connect_timeout,
            request_timeout,
            tls_config,
            sasl_config,
        })
    }
}

fn parse_timeout_ms(key: &str, value: &str) -> anyhow::Result<Option<Duration>> {
    let millis = value
        .parse::<u64>()
        .with_context(|| format!("invalid integer value for {key}: '{value}'"))?;
    Ok(Some(Duration::from_millis(millis)))
}
