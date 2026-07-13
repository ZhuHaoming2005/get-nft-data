use super::*;

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("at least one parquet input is required")]
    MissingParquetInput,
    #[error("invalid data: {0}")]
    InvalidData(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct AnalysisOptions {
    pub database_path: PathBuf,
    pub parquet_inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub name_threshold: f64,
    #[serde(default)]
    pub metadata_recall_mode: MetadataRecallMode,
    pub threads: usize,
    /// Total analysis memory budget string (DuckDB-oriented / overall planning).
    pub memory_limit: String,
    /// Optional hard Rust-side analysis cap. The CLI sets both this and
    /// `memory_limit` to the same `--analysis-memory-limit` value.
    pub analysis_memory_limit: Option<String>,
    pub duckdb_memory_limit: String,
    pub temp_directory: Option<PathBuf>,
    pub progress: bool,
}

#[derive(
    Clone, Copy, Debug, Default, Serialize, serde::Deserialize, PartialEq, Eq, clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub enum MetadataRecallMode {
    Exact,
    #[default]
    Conservative,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct SummaryRow {
    pub field_name: String,
    pub scope: String,
    pub primary_chain: String,
    pub secondary_chain: String,
    pub threshold: Option<f64>,
    pub match_mode: String,
    pub metric: String,
    pub total_contracts: i64,
    pub total_nfts: i64,
    pub group_count: i64,
    pub duplicate_contract_count: i64,
    pub duplicate_nft_count: i64,
    pub duplicate_contract_ratio: f64,
    pub duplicate_nft_ratio: f64,
    pub group_size_ge_2_count: i64,
    pub group_size_gt_2_count: i64,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize, PartialEq)]
pub struct AnalysisReport {
    pub summary_rows: Vec<SummaryRow>,
}

#[derive(Clone, Debug)]
pub(crate) struct NameAtom {
    pub(crate) chain_index: usize,
    pub(crate) name_norm: String,
    pub(crate) char_len: usize,
    pub(crate) contract_count: i64,
    pub(crate) nft_count: i64,
}

pub(crate) const SPARSE_UNION_NODE_BYTES: usize = 96;

#[derive(Clone, Copy)]
pub(crate) struct ScoredRight {
    pub(crate) right: usize,
    pub(crate) score: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct NameTotals {
    pub(crate) contracts: i64,
    pub(crate) nfts: i64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct UriCounts {
    pub(crate) v1_nfts: i64,
    pub(crate) v1_contracts: i64,
    pub(crate) v2_nfts: i64,
    pub(crate) v2_contracts: i64,
    pub(crate) v3_nfts: i64,
    pub(crate) v3_contracts: i64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct UriContractCounts {
    pub(crate) intra_chain: UriCounts,
    pub(crate) cross_chain: UriCounts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct GroupSummary {
    pub(crate) group_count: i64,
    pub(crate) duplicate_contract_count: i64,
    pub(crate) duplicate_nft_count: i64,
    pub(crate) group_size_ge_2_count: i64,
    pub(crate) group_size_gt_2_count: i64,
}

pub(crate) struct SummarySpec<'a> {
    pub(crate) field_name: &'a str,
    pub(crate) scope: &'a str,
    pub(crate) primary_chain: &'a str,
    pub(crate) secondary_chain: &'a str,
    pub(crate) threshold: Option<f64>,
    pub(crate) match_mode: &'a str,
    pub(crate) metric: &'a str,
    pub(crate) total_contracts: i64,
    pub(crate) total_nfts: i64,
}

pub(crate) struct ChainMatrixRowSpec<'a> {
    pub(crate) chains: &'a [String],
    pub(crate) totals: &'a HashMap<String, NameTotals>,
    pub(crate) primary_index: usize,
    pub(crate) secondary_index: usize,
    pub(crate) threshold: f64,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PairComponentAccumulator {
    pub(crate) left_contract_count: i64,
    pub(crate) left_nft_count: i64,
    pub(crate) right_contract_count: i64,
    pub(crate) right_nft_count: i64,
    pub(crate) total_contract_count: i64,
}

pub(crate) struct UnionFind {
    pub(crate) parent: Vec<usize>,
    pub(crate) rank: Vec<u8>,
}

impl UnionFind {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    pub(crate) fn find(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    pub(crate) fn union(&mut self, left: usize, right: usize) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return;
        }
        if self.rank[left_root] < self.rank[right_root] {
            self.parent[left_root] = right_root;
        } else if self.rank[left_root] > self.rank[right_root] {
            self.parent[right_root] = left_root;
        } else {
            self.parent[right_root] = left_root;
            self.rank[left_root] += 1;
        }
    }
}

pub(crate) struct ThresholdUnionState {
    pub(crate) threshold: f64,
    pub(crate) intra: UnionFind,
    pub(crate) cross: Option<SparseUnionFind>,
    pub(crate) chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Default)]
pub(crate) struct SparseUnionFind {
    pub(crate) index_by_atom: HashMap<usize, usize>,
    pub(crate) atoms: Vec<usize>,
    pub(crate) parent: Vec<usize>,
    pub(crate) rank: Vec<u8>,
}

impl SparseUnionFind {
    pub(crate) fn get_or_insert(&mut self, atom: usize) -> usize {
        if let Some(index) = self.index_by_atom.get(&atom).copied() {
            return index;
        }

        let index = self.atoms.len();
        self.index_by_atom.insert(atom, index);
        self.atoms.push(atom);
        self.parent.push(index);
        self.rank.push(0);
        index
    }

    pub(crate) fn find_local(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find_local(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    pub(crate) fn union(&mut self, left: usize, right: usize) {
        let left = self.get_or_insert(left);
        let right = self.get_or_insert(right);

        let left_root = self.find_local(left);
        let right_root = self.find_local(right);
        if left_root == right_root {
            return;
        }

        let left_rank = self.rank[left_root];
        let right_rank = self.rank[right_root];
        if left_rank < right_rank {
            self.parent[left_root] = right_root;
        } else if left_rank > right_rank {
            self.parent[right_root] = left_root;
        } else {
            self.parent[right_root] = left_root;
            self.rank[left_root] += 1;
        }
    }

    pub(crate) fn connected(&mut self, left: usize, right: usize) -> bool {
        let Some(left) = self.index_by_atom.get(&left).copied() else {
            return false;
        };
        let Some(right) = self.index_by_atom.get(&right).copied() else {
            return false;
        };
        self.find_local(left) == self.find_local(right)
    }

    pub(crate) fn atom_count(&self) -> usize {
        self.atoms.len()
    }

    pub(crate) fn atom_at(&self, local_index: usize) -> usize {
        self.atoms[local_index]
    }
}
