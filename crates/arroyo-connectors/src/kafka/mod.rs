use anyhow::{anyhow, bail};
use arrow::datatypes::DataType;
use arroyo_formats::de::ArrowDeserializer;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::connector::{Connection, MetadataDef};
use arroyo_rpc::api_types::connections::{ConnectionProfile, ConnectionSchema, TestSourceMessage};
use arroyo_rpc::df::ArroyoSchema;
use arroyo_rpc::formats::{BadData, Format, JsonFormat};
use arroyo_rpc::schema_resolver::{
    ConfluentSchemaRegistry, ConfluentSchemaRegistryClient, SchemaResolver,
};
use arroyo_rpc::{ConnectorOptions, OperatorConfig, schema_resolver, var_str::VarStr};
use arroyo_types::string_to_map;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tonic::Status;
use tracing::{info, warn};
use typify::import_types;

use crate::{ConnectionType, send};

use crate::kafka::sink::KafkaSinkFunc;
use crate::kafka::source::KafkaSourceFunc;
use arroyo_operator::connector::Connector;
use arroyo_operator::operator::ConstructedOperator;

mod client_utils;
mod sink;
mod source;

use client_utils::{
    build_client, fetch_topics as fetch_topics_with_client, partition_client,
    spawn_partition_consumers, start_offset, topic_metadata as fetch_topic_metadata,
};

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./kafka.svg");

import_types!(
    schema = "src/kafka/profile.json",
    convert = {
        {type = "string", format = "var-str"} = VarStr
    }
);

import_types!(schema = "src/kafka/table.json");

impl KafkaTable {
    pub fn subject(&self) -> Cow<'_, str> {
        match &self.value_subject {
            None => Cow::Owned(format!("{}-value", self.topic)),
            Some(s) => Cow::Borrowed(s),
        }
    }
}

pub struct KafkaConnector {}

impl KafkaConnector {
    pub fn connection_from_options(options: &mut ConnectorOptions) -> anyhow::Result<KafkaConfig> {
        let auth = options.pull_opt_str("auth.type")?;
        let auth = match auth.as_deref() {
            Some("none") | None => KafkaConfigAuthentication::None {},
            Some("sasl") => KafkaConfigAuthentication::Sasl {
                mechanism: options.pull_str("auth.mechanism")?,
                protocol: options.pull_str("auth.protocol")?,
                username: VarStr::new(options.pull_str("auth.username")?),
                password: VarStr::new(options.pull_str("auth.password")?),
            },
            Some("aws_msk_iam") => KafkaConfigAuthentication::AwsMskIam {
                region: options.pull_str("auth.region")?,
            },
            Some(other) => bail!("unknown auth type '{}'", other),
        };

        let schema_registry = options
            .pull_opt_str("schema_registry.endpoint")?
            .map(|endpoint| {
                let api_key = options
                    .pull_opt_str("schema_registry.api_key")?
                    .map(VarStr::new);
                let api_secret = options
                    .pull_opt_str("schema_registry.api_secret")?
                    .map(VarStr::new);
                datafusion::common::Result::<_>::Ok(SchemaRegistry::ConfluentSchemaRegistry {
                    endpoint,
                    api_key,
                    api_secret,
                })
            })
            .transpose()?;

        Ok(KafkaConfig {
            authentication: auth,
            bootstrap_servers: BootstrapServers(options.pull_str("bootstrap_servers")?),
            schema_registry_enum: schema_registry,
            connection_properties: HashMap::new(),
        })
    }

    pub fn table_from_options(options: &mut ConnectorOptions) -> anyhow::Result<KafkaTable> {
        let typ = options.pull_str("type")?;
        let table_type = match typ.as_str() {
            "source" => {
                let offset = options.pull_opt_str("source.offset")?;
                TableType::Source {
                    offset: match offset.as_deref() {
                        Some("earliest") => SourceOffset::Earliest,
                        Some("group") => SourceOffset::Group,
                        None | Some("latest") => SourceOffset::Latest,
                        Some(other) => bail!("invalid value for source.offset '{}'", other),
                    },
                    read_mode: match options.pull_opt_str("source.read_mode")?.as_deref() {
                        Some("read_committed") => Some(ReadMode::ReadCommitted),
                        Some("read_uncommitted") | None => Some(ReadMode::ReadUncommitted),
                        Some(other) => bail!("invalid value for source.read_mode '{}'", other),
                    },
                    group_id: options.pull_opt_str("source.group_id")?,
                    group_id_prefix: options.pull_opt_str("source.group_id_prefix")?,
                }
            }
            "sink" => {
                let commit_mode = options.pull_opt_str("sink.commit_mode")?;
                TableType::Sink {
                    commit_mode: match commit_mode.as_deref() {
                        Some("at_least_once") | None => SinkCommitMode::AtLeastOnce,
                        Some("exactly_once") => SinkCommitMode::ExactlyOnce,
                        Some(other) => bail!("invalid value for commit_mode '{}'", other),
                    },
                    timestamp_field: options.pull_opt_str("sink.timestamp_field")?,
                    key_field: options.pull_opt_str("sink.key_field")?,
                }
            }
            _ => {
                bail!("type must be one of 'source' or 'sink")
            }
        };

        Ok(KafkaTable {
            topic: options.pull_str("topic")?,
            type_: table_type,
            client_configs: options
                .pull_opt_str("client_configs")?
                .map(|c| {
                    string_to_map(&c, '=').ok_or_else(|| {
                        anyhow!("invalid client_config: expected comma and equals-separated pairs")
                    })
                })
                .transpose()?
                .unwrap_or_else(HashMap::new),
            value_subject: options.pull_opt_str("value.subject")?,
        })
    }
}

impl Connector for KafkaConnector {
    type ProfileT = KafkaConfig;
    type TableT = KafkaTable;

    fn name(&self) -> &'static str {
        "kafka"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "kafka".to_string(),
            name: "Kafka".to_string(),
            icon: ICON.to_string(),
            description: "Read and write from a Kafka cluster".to_string(),
            enabled: true,
            source: true,
            sink: true,
            testing: true,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn config_description(&self, config: Self::ProfileT) -> String {
        (*config.bootstrap_servers).clone()
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: KafkaConfig,
        table: KafkaTable,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let (typ, desc) = match table.type_ {
            TableType::Source { .. } => (
                ConnectionType::Source,
                format!("KafkaSource<{}>", table.topic),
            ),
            TableType::Sink { .. } => (ConnectionType::Sink, format!("KafkaSink<{}>", table.topic)),
        };

        let schema = schema
            .map(|s| s.to_owned())
            .ok_or_else(|| anyhow!("No schema defined for Kafka connection"))?;

        let format = schema
            .format
            .as_ref()
            .map(|t| t.to_owned())
            .ok_or_else(|| anyhow!("'format' must be set for Kafka connection"))?;

        let config = OperatorConfig {
            connection: serde_json::to_value(config).unwrap(),
            table: serde_json::to_value(table).unwrap(),
            rate_limit: None,
            format: Some(format),
            bad_data: schema.bad_data.clone(),
            framing: schema.framing.clone(),
            metadata_fields: schema.metadata_fields(),
        };

        Ok(Connection::new(
            id,
            self.name(),
            name.to_string(),
            typ,
            schema,
            &config,
            desc,
        ))
    }

    fn get_autocomplete(
        &self,
        profile: Self::ProfileT,
    ) -> oneshot::Receiver<anyhow::Result<HashMap<String, Vec<String>>>> {
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let kafka = KafkaTester {
                connection: profile,
            };

            tx.send(
                kafka
                    .fetch_topics()
                    .await
                    .map_err(|e| anyhow!("Failed to fetch topics from Kafka: {:?}", e))
                    .map(|topics| {
                        let mut map = HashMap::new();
                        map.insert(
                            "topic".to_string(),
                            topics
                                .into_iter()
                                .map(|(name, _)| name)
                                .filter(|name| !name.starts_with('_'))
                                .collect(),
                        );
                        map
                    }),
            )
            .unwrap();
        });

        rx
    }

    fn test_profile(&self, profile: Self::ProfileT) -> Option<Receiver<TestSourceMessage>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let tester = KafkaTester {
                connection: profile,
            };

            let mut message = tester.test_connection().await;

            if !message.error
                && let Err(e) = tester.test_schema_registry().await
            {
                message.error = true;
                message.message = format!("Failed to connect to schema registry: {e:?}");
            }

            message.done = true;
            tx.send(message).unwrap();
        });

        Some(rx)
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        let tester = KafkaTester { connection: config };

        tester.start(table, schema.cloned(), tx);
    }

    fn table_type(&self, _: Self::ProfileT, table: Self::TableT) -> ConnectionType {
        match table.type_ {
            TableType::Source { .. } => ConnectionType::Source,
            TableType::Sink { .. } => ConnectionType::Sink,
        }
    }

    fn metadata_defs(&self) -> &'static [MetadataDef] {
        &[
            MetadataDef {
                name: "offset_id",
                data_type: DataType::Int64,
            },
            MetadataDef {
                name: "partition",
                data_type: DataType::Int32,
            },
            MetadataDef {
                name: "topic",
                data_type: DataType::Utf8,
            },
            MetadataDef {
                name: "timestamp",
                data_type: DataType::Int64,
            },
            MetadataDef {
                name: "key",
                data_type: DataType::Binary,
            },
        ]
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let connection = profile
            .map(|p| {
                serde_json::from_value(p.config.clone()).map_err(|e| {
                    anyhow!("invalid config for profile '{}' in database: {}", p.id, e)
                })
            })
            .unwrap_or_else(|| Self::connection_from_options(options))?;

        let table = Self::table_from_options(options)?;

        Self::from_config(self, None, name, connection, table, schema)
    }

    fn make_operator(
        &self,
        profile: Self::ProfileT,
        table: Self::TableT,
        config: OperatorConfig,
    ) -> anyhow::Result<ConstructedOperator> {
        match &table.type_ {
            TableType::Source {
                group_id,
                offset,
                read_mode,
                group_id_prefix,
            } => {
                if matches!(read_mode, Some(ReadMode::ReadCommitted)) {
                    bail!(
                        "Kafka sources configured with source.read_mode='read_committed' are not supported by the rskafka client"
                    );
                }
                let client_configs = client_configs(&profile, Some(table.clone()))?;

                let schema_resolver: Option<Arc<dyn SchemaResolver + Sync>> =
                    if let Some(SchemaRegistry::ConfluentSchemaRegistry {
                        endpoint,
                        api_key,
                        api_secret,
                    }) = &profile.schema_registry_enum
                    {
                        Some(Arc::new(
                            ConfluentSchemaRegistry::new(
                                endpoint,
                                &table.subject(),
                                api_key.clone(),
                                api_secret.clone(),
                            )
                            .expect("failed to construct confluent schema resolver"),
                        ))
                    } else {
                        None
                    };

                Ok(ConstructedOperator::from_source(Box::new(
                    KafkaSourceFunc {
                        topic: table.topic,
                        bootstrap_servers: profile.bootstrap_servers.to_string(),
                        group_id: group_id.clone(),
                        group_id_prefix: group_id_prefix.clone(),
                        offset_mode: *offset,
                        format: config.format.expect("Format must be set for Kafka source"),
                        framing: config.framing,
                        schema_resolver,
                        bad_data: config.bad_data,
                        client_configs,
                        context: Context::new(Some(profile.clone())),
                        messages_per_second: NonZeroU32::new(
                            config
                                .rate_limit
                                .map(|l| l.messages_per_second)
                                .unwrap_or(u32::MAX),
                        )
                        .unwrap(),
                        metadata_fields: config.metadata_fields,
                    },
                )))
            }
            TableType::Sink {
                commit_mode,
                key_field,
                timestamp_field,
            } => {
                if matches!(commit_mode, SinkCommitMode::ExactlyOnce) {
                    bail!(
                        "Kafka sinks configured with sink.commit_mode='exactly_once' are not supported by the rskafka client"
                    );
                }

                Ok(ConstructedOperator::from_operator(Box::new(
                    KafkaSinkFunc {
                        bootstrap_servers: profile.bootstrap_servers.to_string(),
                        producer: None,
                        consistency_mode: (*commit_mode).into(),
                        timestamp_field: timestamp_field.clone(),
                        timestamp_col: None,
                        key_field: key_field.clone(),
                        key_col: None,
                        client_config: client_configs(&profile, Some(table.clone()))?,
                        context: Context::new(Some(profile.clone())),
                        topic: table.topic,
                        serializer: ArrowSerializer::new(
                            config.format.expect("Format must be defined for KafkaSink"),
                        ),
                    },
                )))
            }
        }
    }
}

pub struct KafkaTester {
    pub connection: KafkaConfig,
}

pub struct TopicMetadata {
    pub partitions: usize,
}

impl KafkaTester {
    async fn connect(&self, table: Option<KafkaTable>) -> Result<rskafka::client::Client, String> {
        build_client(
            &self.connection.bootstrap_servers.to_string(),
            &client_configs(&self.connection, table).map_err(|e| e.to_string())?,
            &Context::new(Some(self.connection.clone())),
        )
        .await
        .map_err(|e| e.to_string())
    }

    #[allow(unused)]
    #[allow(clippy::result_large_err)]
    pub async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, Status> {
        let client = self
            .connect(None)
            .await
            .map_err(Status::failed_precondition)?;
        let metadata = fetch_topic_metadata(&client, topic)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("Topic does not exist"))?;

        Ok(TopicMetadata {
            partitions: metadata.partitions.len(),
        })
    }

    async fn fetch_topics(&self) -> anyhow::Result<Vec<(String, usize)>> {
        let client = self.connect(None).await.map_err(|e| anyhow!("{e}"))?;
        fetch_topics_with_client(&client).await
    }

    pub async fn test_schema_registry(&self) -> anyhow::Result<()> {
        if let Some(SchemaRegistry::ConfluentSchemaRegistry {
            api_key,
            api_secret,
            endpoint,
        }) = &self.connection.schema_registry_enum
        {
            let client =
                ConfluentSchemaRegistryClient::new(endpoint, api_key.clone(), api_secret.clone())?;

            client.test().await?;
        }

        Ok(())
    }

    pub async fn validate_schema(
        &self,
        table: &KafkaTable,
        schema: &ConnectionSchema,
        format: &Format,
        msg: Vec<u8>,
    ) -> anyhow::Result<()> {
        match format {
            Format::Json(JsonFormat {
                confluent_schema_registry,
                ..
            }) => {
                if *confluent_schema_registry {
                    if msg[0] != 0 {
                        bail!(
                            "Message appears to be encoded as normal JSON, rather than SR-JSON, but the schema registry is enabled. Ensure that the format and schema type are correct."
                        );
                    }
                    serde_json::from_slice::<Value>(&msg[5..]).map_err(|e|
                        anyhow!("Failed to parse message as schema-registry JSON (SR-JSON): {:?}. Ensure that the format and schema type are correct.", e))?;
                } else if msg[0] == 0 {
                    bail!(
                        "Message is not valid JSON. It may be encoded as SR-JSON, but the schema registry is not enabled. Ensure that the format and schema type are correct."
                    );
                } else {
                    serde_json::from_slice::<Value>(&msg).map_err(|e|
                        anyhow!("Failed to parse message as JSON: {:?}. Ensure that the format and schema type are correct.", e))?;
                }
            }
            Format::Avro(avro) => {
                if avro.confluent_schema_registry {
                    let schema_resolver = match &self.connection.schema_registry_enum {
                        Some(SchemaRegistry::ConfluentSchemaRegistry {
                            endpoint,
                            api_key,
                            api_secret,
                        }) => schema_resolver::ConfluentSchemaRegistry::new(
                            endpoint,
                            &table.subject(),
                            api_key.clone(),
                            api_secret.clone(),
                        ),
                        _ => {
                            bail!(
                                "schema registry is enabled, but no schema registry is configured"
                            );
                        }
                    }
                    .map_err(|e| anyhow!("Failed to construct schema registry: {:?}", e))?;

                    if msg[0] != 0 {
                        bail!(
                            "Message appears to be encoded as normal Avro, rather than SR-Avro, but the schema registry is enabled. Ensure that the format and schema type are correct."
                        );
                    }

                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::with_schema_resolver(
                        format.clone(),
                        None,
                        Arc::new(aschema),
                        &schema.metadata_fields(),
                        BadData::Fail {},
                        Arc::new(schema_resolver),
                    );

                    let mut error = deserializer
                        .deserialize_slice(&msg, SystemTime::now(), None)
                        .await
                        .into_iter()
                        .next();
                    if let Some(e) = deserializer.flush_buffer().1.pop() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        bail!(
                            "Failed to parse message as schema-registry Avro (SR-Avro): {}. Ensure that the format and schema type are correct.",
                            error
                        );
                    }
                } else {
                    let aschema: ArroyoSchema = schema.clone().into();
                    let mut deserializer = ArrowDeserializer::new(
                        format.clone(),
                        Arc::new(aschema),
                        &schema.metadata_fields(),
                        None,
                        BadData::Fail {},
                    );

                    let mut error = deserializer
                        .deserialize_slice(&msg, SystemTime::now(), None)
                        .await
                        .into_iter()
                        .next();
                    if let Some(e) = deserializer.flush_buffer().1.pop() {
                        error.replace(e);
                    }

                    if let Some(error) = error {
                        if msg[0] == 0 {
                            bail!(
                                "Failed to parse message as regular Avro. It may be encoded as SR-Avro, but the schema registry is not enabled. Ensure that the format and schema type are correct."
                            );
                        } else {
                            bail!(
                                "Failed to parse message as Avro: {}. Ensure that the format and schema type are correct.",
                                error
                            );
                        };
                    }
                }
            }
            Format::Parquet(_) => {
                unreachable!()
            }
            Format::RawString(_) => {
                String::from_utf8(msg).map_err(|e|
                    anyhow!("Failed to parse message as UTF-8: {:?}. Ensure that the format and schema type are correct.", e))?;
            }
            Format::RawBytes(_) => {
                // all bytes are valid
            }
            Format::Flatbuffers(_) => {
                bail!("Flatbuffers messages are only supported by the NATS connector");
            }
            Format::Protobuf(_) => {
                let aschema: ArroyoSchema = schema.clone().into();
                let mut deserializer = ArrowDeserializer::new(
                    format.clone(),
                    Arc::new(aschema),
                    &schema.metadata_fields(),
                    None,
                    BadData::Fail {},
                );

                let mut error = deserializer
                    .deserialize_slice(&msg, SystemTime::now(), None)
                    .await
                    .into_iter()
                    .next();
                if let Some(e) = deserializer.flush_buffer().1.pop() {
                    error.replace(e);
                }

                if let Some(error) = error {
                    bail!(
                        "Failed to parse message according to the provided Protobuf schema: {}",
                        error
                    );
                }
            }
        };

        Ok(())
    }

    async fn test(
        &self,
        table: KafkaTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) -> anyhow::Result<()> {
        let format = schema
            .as_ref()
            .and_then(|s| s.format.clone())
            .ok_or_else(|| anyhow!("No format defined for Kafka connection"))?;

        let client = self
            .connect(Some(table.clone()))
            .await
            .map_err(|e| anyhow!("{}", e))?;

        self.info(&mut tx, "Connected to Kafka").await;

        let topic = table.topic.clone();
        let metadata = fetch_topic_metadata(&client, &topic)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "Topic '{}' does not exist in the configured Kafka cluster",
                    topic
                )
            })?;

        self.info(&mut tx, "Fetched topic metadata").await;

        if let TableType::Source { .. } = table.type_ {
            self.info(&mut tx, "Waiting for messages").await;

            let mut consumers = Vec::with_capacity(metadata.partitions.len());
            for partition in &metadata.partitions {
                consumers.push((
                    *partition,
                    partition_client(&client, &topic, *partition).await?,
                    start_offset(SourceOffset::Earliest, None)?,
                ));
            }
            let (mut rx, handles) = spawn_partition_consumers(&topic, consumers);
            let start = Instant::now();
            let timeout = Duration::from_secs(30);
            while start.elapsed() < timeout {
                if let Ok(Some(message)) =
                    tokio::time::timeout(Duration::from_millis(500), rx.recv()).await
                {
                    let message = message?;
                    if let Some(payload) = message.record.record.value {
                        self.info(&mut tx, "Received message from Kafka").await;
                        self.validate_schema(&table, schema.as_ref().unwrap(), &format, payload)
                            .await?;

                        self.info(&mut tx, "Successfully validated message schema")
                            .await;
                        for handle in handles {
                            handle.abort();
                        }
                        return Ok(());
                    }
                }
            }

            for handle in handles {
                handle.abort();
            }

            return Err(anyhow!(
                "No messages received from Kafka within {} seconds",
                timeout.as_secs()
            ));
        }

        Ok(())
    }

    async fn info(&self, tx: &mut Sender<TestSourceMessage>, s: impl Into<String>) {
        send(
            tx,
            TestSourceMessage {
                error: false,
                done: false,
                message: s.into(),
            },
        )
        .await;
    }

    #[allow(unused)]
    pub async fn test_connection(&self) -> TestSourceMessage {
        match self.connect(None).await {
            Ok(_) => TestSourceMessage {
                error: false,
                done: true,
                message: "Successfully connected to Kafka".to_string(),
            },
            Err(e) => TestSourceMessage {
                error: true,
                done: true,
                message: e,
            },
        }
    }

    pub fn start(
        self,
        table: KafkaTable,
        schema: Option<ConnectionSchema>,
        mut tx: Sender<TestSourceMessage>,
    ) {
        tokio::spawn(async move {
            info!("Started kafka tester");
            if let Err(e) = self.test(table, schema, tx.clone()).await {
                send(
                    &mut tx,
                    TestSourceMessage {
                        error: true,
                        done: true,
                        message: e.to_string(),
                    },
                )
                .await;
            } else {
                send(
                    &mut tx,
                    TestSourceMessage {
                        error: false,
                        done: true,
                        message: "Connection is valid".to_string(),
                    },
                )
                .await;
            }
        });
    }
}

pub fn client_configs(
    connection: &KafkaConfig,
    table: Option<KafkaTable>,
) -> anyhow::Result<HashMap<String, String>> {
    let mut client_configs: HashMap<String, String> = HashMap::new();

    match &connection.authentication {
        KafkaConfigAuthentication::None {} => {}
        KafkaConfigAuthentication::Sasl {
            mechanism,
            password,
            protocol,
            username,
        } => {
            client_configs.insert("sasl.mechanism".to_string(), mechanism.to_string());
            client_configs.insert("security.protocol".to_string(), protocol.to_string());
            client_configs.insert("sasl.username".to_string(), username.sub_env_vars()?);
            client_configs.insert("sasl.password".to_string(), password.sub_env_vars()?);
        }
        KafkaConfigAuthentication::AwsMskIam { region: _ } => {
            client_configs.insert("sasl.mechanism".to_string(), "OAUTHBEARER".to_string());
            client_configs.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        }
    };

    for (k, v) in connection.connection_properties.iter() {
        client_configs.insert(k.to_string(), v.to_string());
    }

    if let Some(table) = table {
        for (k, v) in table.client_configs.iter() {
            if connection.connection_properties.contains_key(k) {
                warn!(
                    "Kafka config key {:?} defined in both connection and table config",
                    k
                );
            }

            client_configs.insert(k.to_string(), v.to_string());
        }
    }

    Ok(client_configs)
}

#[derive(Clone)]
pub struct Context {
    config: Option<KafkaConfig>,
}

impl Context {
    pub fn new(config: Option<KafkaConfig>) -> Self {
        Self { config }
    }
}
