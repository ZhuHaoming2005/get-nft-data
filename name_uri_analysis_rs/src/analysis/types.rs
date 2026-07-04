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

#[derive(Clone, Debug)]
pub struct AnalysisOptions {
    pub database_path: PathBuf,
    pub parquet_inputs: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub thresholds: Vec<f64>,
    pub threads: usize,
    pub memory_limit: String,
    pub analysis_memory_limit: Option<String>,
    pub temp_directory: Option<PathBuf>,
    pub progress: bool,
    pub persist_prepared: bool,
    pub reuse_prepared: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
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

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AnalysisReport {
    pub summary_rows: Vec<SummaryRow>,
}

#[derive(Clone, Debug)]
struct NameAtom {
    chain_index: usize,
    name_norm: String,
    char_len: usize,
    contract_count: i64,
    nft_count: i64,
}

const RIGHT_SCORE_CHUNK_SIZE: usize = 8192;
const SPARSE_UNION_NODE_BYTES: usize = 96;
const PROGRESS_FLUSH_CHUNKS: u64 = 128;

#[derive(Clone, Copy)]
struct ScoredRight {
    right: usize,
    score: f64,
}

#[derive(Clone, Copy)]
struct NameTotals {
    contracts: i64,
    nfts: i64,
}

#[derive(Clone, Copy, Default)]
struct UriCounts {
    v1_nfts: i64,
    v1_contracts: i64,
    v2_nfts: i64,
    v2_contracts: i64,
    v3_nfts: i64,
    v3_contracts: i64,
}

#[derive(Clone, Copy, Default)]
struct UriContractCounts {
    intra_chain: UriCounts,
    cross_chain: UriCounts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct GroupSummary {
    group_count: i64,
    duplicate_contract_count: i64,
    duplicate_nft_count: i64,
    group_size_ge_2_count: i64,
    group_size_gt_2_count: i64,
}

struct SummarySpec<'a> {
    field_name: &'a str,
    scope: &'a str,
    primary_chain: &'a str,
    secondary_chain: &'a str,
    threshold: Option<f64>,
    match_mode: &'a str,
    metric: &'a str,
    total_contracts: i64,
    total_nfts: i64,
}

struct ChainMatrixRowSpec<'a> {
    chains: &'a [String],
    totals: &'a HashMap<String, NameTotals>,
    primary_index: usize,
    secondary_index: usize,
    threshold: f64,
}

struct ChainMatrixAnalysisSpec<'a> {
    thresholds: &'a [f64],
    analysis_budget: usize,
    total_memory_budget: usize,
    totals: &'a HashMap<String, NameTotals>,
}

struct MatrixUnionState {
    threshold: f64,
    union_find: SparseUnionFind,
}

struct ChainMatrixReusePlan {
    per_threshold_bytes: usize,
    pair_count: usize,
}

#[derive(Clone, Copy, Default)]
struct PairComponentAccumulator {
    left_contract_count: i64,
    left_nft_count: i64,
    right_contract_count: i64,
    right_nft_count: i64,
    total_contract_count: i64,
}

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) {
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

struct ThresholdUnionState {
    threshold: f64,
    intra: UnionFind,
    cross: Option<SparseUnionFind>,
    chain_matrix: Option<Vec<SparseUnionFind>>,
}

#[derive(Default)]
struct SparseUnionFind {
    index_by_atom: HashMap<usize, usize>,
    atoms: Vec<usize>,
    parent: Vec<usize>,
    rank: Vec<u8>,
}

impl SparseUnionFind {
    fn get_or_insert(&mut self, atom: usize) -> usize {
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

    fn find_local(&mut self, node: usize) -> usize {
        let parent = self.parent[node];
        if parent != node {
            let root = self.find_local(parent);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) {
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

    fn connected(&mut self, left: usize, right: usize) -> bool {
        let Some(left) = self.index_by_atom.get(&left).copied() else {
            return false;
        };
        let Some(right) = self.index_by_atom.get(&right).copied() else {
            return false;
        };
        self.find_local(left) == self.find_local(right)
    }

    fn atom_count(&self) -> usize {
        self.atoms.len()
    }

    fn atom_at(&self, local_index: usize) -> usize {
        self.atoms[local_index]
    }
}
