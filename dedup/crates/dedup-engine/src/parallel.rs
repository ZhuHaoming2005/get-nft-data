use dedup_model::{ChunkExecutor, DedupError, ErrorContext};
use rayon::prelude::*;

pub(crate) struct RayonChunkExecutor {
    workers: usize,
}

impl RayonChunkExecutor {
    pub(crate) fn new(workers: usize, stage: &'static str) -> Result<Self, DedupError> {
        if workers == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage(stage),
                message: "worker count must be positive".to_owned(),
            });
        }
        Ok(Self { workers })
    }
}

impl ChunkExecutor for RayonChunkExecutor {
    fn worker_count(&self) -> usize {
        self.workers
    }

    fn map_chunks<T, R, F>(
        &self,
        items: &[T],
        chunk_size: usize,
        map: F,
    ) -> Result<Vec<R>, DedupError>
    where
        T: Sync,
        R: Send,
        F: Fn(&[T]) -> Result<R, DedupError> + Send + Sync,
    {
        if chunk_size == 0 {
            return Err(DedupError::InvalidInput {
                context: ErrorContext::stage("parallel"),
                message: "chunk size must be positive".to_owned(),
            });
        }
        if self.workers == 1 {
            return items.chunks(chunk_size).map(map).collect();
        }
        let collect = || items.par_chunks(chunk_size).map(&map).collect::<Vec<_>>();
        let chunks = if rayon::current_thread_index().is_some() {
            collect()
        } else {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.workers)
                .thread_name(|index| format!("dedup-worker-{index}"))
                .build()
                .map_err(|error| DedupError::InvalidInput {
                    context: ErrorContext::stage("parallel"),
                    message: error.to_string(),
                })?
                .install(collect)
        };
        chunks.into_iter().collect()
    }
}
