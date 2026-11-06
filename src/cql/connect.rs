use latte_core::config::ConnectionConf;
use latte_core::scripting::db_config::{Consistency, SerialConsistency};
use latte_core::version::get_version_info;
use openssl::ssl::{SslContextBuilder, SslFiletype, SslMethod, SslVerifyMode};
use scylla::client::session::{OpenSsl010Config, TlsContext};
use scylla::client::{PoolSize, SelfIdentity};
use scylla::policies::load_balancing::DefaultPolicy;

use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::routing::ShardAwarePortRange;
use scylla::statement::{Consistency as DriverConsistency, SerialConsistency as DriverSerial};

fn tls_context(conf: &ConnectionConf) -> Result<Option<TlsContext>, String> {
    if conf.db.ssl {
        let mut ssl = SslContextBuilder::new(SslMethod::tls()).map_err(ssl_err)?;
        if let Some(path) = &conf.db.ssl_ca_cert_file {
            ssl.set_ca_file(path).map_err(ssl_err)?;
        }
        if let Some(path) = &conf.db.ssl_cert_file {
            ssl.set_certificate_file(path, SslFiletype::PEM)
                .map_err(ssl_err)?;
        }
        if let Some(path) = &conf.db.ssl_key_file {
            ssl.set_private_key_file(path, SslFiletype::PEM)
                .map_err(ssl_err)?;
        }
        if conf.db.ssl_peer_verification {
            ssl.set_verify(SslVerifyMode::PEER);
        }
        // NOTE: `TlsContext::from(ssl.build())` silently resolves to the deprecated `OpenSsl010`
        //       and it cannot bank TLS tickets.
        //       So, use the 'TlsContext::OpenSsl010Config(...)' one with `from_dangerous_builder`
        //       which passes the raw context above.
        //       The `::new` needs an `SslConnectorBuilder` and would layer its own defaults
        //       (system trust store, cipher list, `set_verify(PEER)`) on top of ours.
        Ok(Some(TlsContext::OpenSsl010Config(
            OpenSsl010Config::from_dangerous_builder(ssl),
        )))
    } else {
        Ok(None)
    }
}

fn ssl_err(e: openssl::error::ErrorStack) -> String {
    format!("SSL configuration error: {e}")
}

fn consistency(c: Consistency) -> DriverConsistency {
    match c {
        Consistency::Any => DriverConsistency::Any,
        Consistency::One => DriverConsistency::One,
        Consistency::Two => DriverConsistency::Two,
        Consistency::Three => DriverConsistency::Three,
        Consistency::Quorum => DriverConsistency::Quorum,
        Consistency::All => DriverConsistency::All,
        Consistency::LocalOne => DriverConsistency::LocalOne,
        Consistency::LocalQuorum => DriverConsistency::LocalQuorum,
        Consistency::EachQuorum => DriverConsistency::EachQuorum,
        Consistency::Serial => DriverConsistency::Serial,
        Consistency::LocalSerial => DriverConsistency::LocalSerial,
    }
}

fn serial_consistency(c: SerialConsistency) -> DriverSerial {
    match c {
        SerialConsistency::Serial => DriverSerial::Serial,
        SerialConsistency::LocalSerial => DriverSerial::LocalSerial,
    }
}

/// Connects to the cluster with the settings the core parsed from the command line.
/// The 'client_id' is advertised to the server to tell concurrent latte calls apart.
pub async fn connect(conf: &ConnectionConf, client_id: &str) -> Result<super::Driver, String> {
    let mut policy_builder = DefaultPolicy::builder().token_aware(true);
    if let Some(dc) = &conf.datacenter {
        if let Some(current_rack) = &conf.rack {
            policy_builder = policy_builder
                .prefer_datacenter_and_rack(dc.to_owned(), current_rack.to_owned())
                .permit_dc_failover(true);
        } else {
            policy_builder = policy_builder
                .prefer_datacenter(dc.to_owned())
                .permit_dc_failover(true);
        }
    } else if let Some(_rack) = &conf.rack {
        panic!("Datacenter must also be defined when rack is defined");
    }
    let profile = ExecutionProfile::builder()
        .consistency(consistency(conf.db.consistency))
        .serial_consistency(Some(serial_consistency(conf.db.serial_consistency)))
        .load_balancing_policy(policy_builder.build())
        .request_timeout(Some(conf.request_timeout))
        .build();

    let mut session_builder = SessionBuilder::new()
        .known_nodes(&conf.addresses)
        .pool_size(PoolSize::PerShard(conf.db.count))
        .user(&conf.db.user, &conf.db.password)
        .tls_context(tls_context(conf)?)
        // Makes latte runs tellable apart from any other client in 'system.clients'.
        // The name stays constant to find all latte clients, while 'client_id' separates
        // concurrent latte calls from each other. See the 'connect' callers for its source.
        .custom_identity(
            SelfIdentity::new()
                .with_application_name("latte")
                .with_application_version(get_version_info().latte_version)
                .with_client_id(client_id.to_string()),
        )
        .default_execution_profile_handle(profile.into_handle());
    let range = conf.db.shard_aware_port_range;
    let port_range = ShardAwarePortRange::new(range.start..=range.end)
        .map_err(|e| format!("Invalid shard-aware port range '{range}': {e}"))?;
    session_builder = session_builder.shard_aware_local_port_range(port_range);
    if conf.db.tcp_reuse_address {
        session_builder = session_builder.tcp_reuse_address(true);
    }
    let session = session_builder
        .build()
        .await
        .map_err(|e| format!("Could not connect to {}: {}", conf.addresses.join(","), e))?;
    Ok(super::Driver::new(session))
}

/// The settings this host gave the driver, for the report.
pub fn driver_settings(conf: &ConnectionConf) -> serde_json::Value {
    let version = get_version_info();
    serde_json::json!({
        "driver": "scylla-rust-driver",
        "driver_version": version.db_driver_version,
        "consistency": format!("{:?}", conf.db.consistency),
        "serial_consistency": format!("{:?}", conf.db.serial_consistency),
        "page_size": conf.page_size.get(),
        "connections_per_shard": conf.db.count.get(),
        "request_timeout_ms": conf.request_timeout.as_millis() as u64,
        "load_balancing": "DefaultPolicy, token aware",
        "datacenter": conf.datacenter,
        "rack": conf.rack,
        "tls": conf.db.ssl,
        "shard_aware_port_range": conf.db.shard_aware_port_range.to_string(),
        "tcp_reuse_address": conf.db.tcp_reuse_address,
    })
}
