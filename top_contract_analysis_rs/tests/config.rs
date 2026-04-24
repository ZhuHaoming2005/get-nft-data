use top_contract_analysis_rs::config::postgres_connection_config;
use top_contract_analysis_rs::constants::{
    DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER,
};

#[test]
fn postgres_connection_config_uses_constants_file_values() {
    let config = postgres_connection_config();

    assert_eq!(
        config,
        format!(
            "host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS} connect_timeout={DB_CONNECT_TIMEOUT}"
        )
    );
}
