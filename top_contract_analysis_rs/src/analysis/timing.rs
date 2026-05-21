use std::future::Future;
use std::time::{Duration, Instant};

pub(crate) fn format_timing(label: &str, elapsed: Duration) -> String {
    format!("[timing] {label} elapsed_ms={}", elapsed.as_millis())
}

pub(crate) fn log_timing(label: &str, started: Instant) {
    eprintln!("{}", format_timing(label, started.elapsed()));
}

pub(crate) async fn time_async<T, Fut>(label: impl Into<String>, future: Fut) -> T
where
    Fut: Future<Output = T>,
{
    let label = label.into();
    eprintln!("[timing] {label} started");
    let started = Instant::now();
    let output = future.await;
    log_timing(&label, started);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_timing_includes_stage_label_and_elapsed_millis() {
        assert_eq!(
            format_timing("seed:0xseed:load_snapshot", Duration::from_millis(42)),
            "[timing] seed:0xseed:load_snapshot elapsed_ms=42"
        );
    }
}
