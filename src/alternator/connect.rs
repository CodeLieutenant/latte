use super::address::normalize_address;
use super::client::create_client;
use super::Driver;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::error::DisplayErrorContext;
use latte_core::config::ConnectionConf;

/// Connects to Alternator and checks the connection with one `ListTables` call.
///
/// The 'client_id' is unused here.
/// Alternator speaks HTTP/DynamoDB and is not tracked in 'system.clients' at all.
/// The driver's only identity knob is 'AlternatorBuilder:: app_name',
/// which rides in the User-Agent where nothing can query it - and 'AppName'
/// rejects ':' and ',', so the 'fn--' ids would not even construct.
pub async fn connect(conf: &ConnectionConf, _client_id: &str) -> Result<Driver, String> {
    let address = conf
        .addresses
        .iter()
        .find_map(|addr| normalize_address(addr))
        .ok_or_else(|| {
            format!(
                "Failed to connect to Alternator at {}: no valid address",
                conf.addresses.join(", ")
            )
        })?
        .to_string();

    let mut config_loader = aws_config::defaults(BehaviorVersion::latest())
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .operation_timeout(conf.request_timeout)
                .build(),
        );

    // We only specify custom credentials if aws_credentials flag is not set.
    // If aws_credentials flag is set, the SDK will automatically use credentials from the environment.
    if !conf.db.aws_credentials {
        let creds = Credentials::new(
            &conf.db.access_key_id,
            &conf.db.secret_access_key,
            None,
            None,
            "",
        );

        config_loader = config_loader
            .credentials_provider(creds)
            .region(Region::new(conf.db.region.clone()));
    }

    let config = config_loader.load().await;

    let client = create_client(&config, conf);

    // Validate connection by making a test request
    client.list_tables().limit(1).send().await.map_err(|e| {
        format!(
            "Failed to connect to Alternator at {}: {}",
            address,
            DisplayErrorContext(&e)
        )
    })?;

    Ok(Driver::new(client))
}

/// The settings this host gave the client, for the report.
pub fn driver_settings(conf: &ConnectionConf) -> serde_json::Value {
    let version = latte_core::version::get_version_info();
    let opts = &conf.alternator;
    serde_json::json!({
        "driver": "alternator-client",
        "driver_version": version.db_driver_version,
        "request_timeout_ms": conf.request_timeout.as_millis() as u64,
        "region": conf.db.region,
        "request_compression": format!("{:?}", opts.request_compression),
        "compression_threshold": opts.compression_threshold,
        "compression_level": opts.compression_level,
        "optimize_headers": opts.optimize_headers,
        "active_interval_ms": opts.active_interval,
        "idle_interval_ms": opts.idle_interval,
        "routing_fallback": opts.routing_fallback,
        "key_route_affinity": opts.key_route_affinity.map(|a| format!("{a:?}")),
        "datacenter": conf.datacenter,
        "rack": conf.rack,
    })
}
