use crate::constants::{DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER};

pub fn postgres_connection_config() -> String {
    format!(
        "host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS} connect_timeout={DB_CONNECT_TIMEOUT}"
    )
}
