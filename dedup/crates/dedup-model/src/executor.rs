use crate::DedupError;

/// Executes bounded slices without exposing platform scheduling to business engines.
pub trait ChunkExecutor {
    fn worker_count(&self) -> usize;

    fn map_chunks<T, R, F>(
        &self,
        items: &[T],
        chunk_size: usize,
        map: F,
    ) -> Result<Vec<R>, DedupError>
    where
        T: Sync,
        R: Send,
        F: Fn(&[T]) -> Result<R, DedupError> + Send + Sync;
}
