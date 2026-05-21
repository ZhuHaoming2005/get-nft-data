#![allow(dead_code)]

// Keep shared fixtures in this parent module so case modules can use private helpers.
include!("support.rs");

mod address_and_sales;
mod candidates;
mod concurrency;
mod reporting;
mod value_flow;
