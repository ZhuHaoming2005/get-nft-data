#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <libpq-fe.h>

struct Settings {
    std::string runLabel;
    std::string workerId = "worker-1";
    double trigramCutoff = 0.35;
    int maxLenDelta = 12;
    std::vector<double> thresholds;
};

struct Task {
    long long id = 0;
    std::string taskKey;
    std::string chainsCsv;
    std::string blockKey;
    std::string signaturePrefix;
    bool valid = false;
};

struct Atom {
    long long atomId = 0;
    std::string chain;
    std::string name;
    long long contractCount = 0;
    long long nftCount = 0;
    std::vector<std::string> trigrams;
};

struct Edge {
    long long leftAtomId = 0;
    long long rightAtomId = 0;
    double similarity = 0.0;
};

static std::string envOr(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    return value ? value : fallback;
}

static std::vector<std::string> split(const std::string& value, char sep) {
    std::vector<std::string> parts;
    std::stringstream stream(value);
    std::string item;
    while (std::getline(stream, item, sep)) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

static std::vector<double> parseThresholds(const std::string& value) {
    std::vector<double> out;
    for (const auto& part : split(value, ',')) {
        out.push_back(std::stod(part));
    }
    if (out.empty()) {
        throw std::runtime_error("thresholds cannot be empty");
    }
    return out;
}

static Settings parseArgs(int argc, char** argv) {
    Settings settings;
    settings.thresholds = {85.0, 90.0, 95.0};
    for (int index = 1; index < argc; ++index) {
        std::string arg = argv[index];
        if (arg == "--run-label" && index + 1 < argc) {
            settings.runLabel = argv[++index];
        } else if (arg == "--worker-id" && index + 1 < argc) {
            settings.workerId = argv[++index];
        } else if (arg == "--trigram-cutoff" && index + 1 < argc) {
            settings.trigramCutoff = std::stod(argv[++index]);
        } else if (arg == "--max-len-delta" && index + 1 < argc) {
            settings.maxLenDelta = std::stoi(argv[++index]);
        } else if (arg == "--thresholds" && index + 1 < argc) {
            settings.thresholds = parseThresholds(argv[++index]);
        }
    }
    if (settings.runLabel.empty()) {
        throw std::runtime_error("--run-label is required");
    }
    return settings;
}

static void ensureOk(PGresult* result, ExecStatusType expected, PGconn* conn) {
    if (PQresultStatus(result) != expected) {
        std::string error = PQerrorMessage(conn);
        PQclear(result);
        throw std::runtime_error(error);
    }
}

static PGconn* connectDb() {
    std::ostringstream dsn;
    dsn << "host=" << envOr("DB_HOST", "localhost")
        << " port=" << envOr("DB_PORT", "5432")
        << " dbname=" << envOr("DB_NAME", "nft_data")
        << " user=" << envOr("DB_USER", "postgres")
        << " password=" << envOr("DB_PASS", "123456");
    PGconn* conn = PQconnectdb(dsn.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string error = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error(error);
    }
    return conn;
}

static std::vector<std::string> buildTrigrams(const std::string& value) {
    std::vector<std::string> out;
    if (value.empty()) {
        return out;
    }
    if (value.size() < 3) {
        out.push_back(value);
        return out;
    }
    out.reserve(value.size() - 2);
    for (size_t index = 0; index + 2 < value.size(); ++index) {
        out.push_back(value.substr(index, 3));
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

static double levenshteinRatio(const std::string& left, const std::string& right) {
    if (left == right) {
        return 100.0;
    }
    if (left.empty() || right.empty()) {
        return 0.0;
    }
    std::vector<int> prev(right.size() + 1);
    std::vector<int> curr(right.size() + 1);
    for (size_t j = 0; j <= right.size(); ++j) {
        prev[j] = static_cast<int>(j);
    }
    for (size_t i = 0; i < left.size(); ++i) {
        curr[0] = static_cast<int>(i + 1);
        for (size_t j = 0; j < right.size(); ++j) {
            int cost = left[i] == right[j] ? 0 : 1;
            curr[j + 1] = std::min({
                prev[j + 1] + 1,
                curr[j] + 1,
                prev[j] + cost,
            });
        }
        std::swap(prev, curr);
    }
    int distance = prev[right.size()];
    int maxLen = static_cast<int>(std::max(left.size(), right.size()));
    return 100.0 * (1.0 - static_cast<double>(distance) / static_cast<double>(maxLen));
}

static double trigramJaccard(const Atom& left, const Atom& right, int shared) {
    int unionSize = static_cast<int>(left.trigrams.size() + right.trigrams.size() - shared);
    if (unionSize <= 0) {
        return 0.0;
    }
    return static_cast<double>(shared) / static_cast<double>(unionSize);
}

static Task claimTask(PGconn* conn, const Settings& settings) {
    PGresult* begin = PQexec(conn, "BEGIN");
    ensureOk(begin, PGRES_COMMAND_OK, conn);
    PQclear(begin);

    const char* values[2] = {settings.runLabel.c_str(), settings.workerId.c_str()};
    PGresult* result = PQexecParams(
        conn,
        "WITH candidate AS ("
        "  SELECT id"
        "  FROM nsv2_name_work_items"
        "  WHERE run_label = $1 AND status = 'pending'"
        "  ORDER BY atom_count DESC, id"
        "  LIMIT 1"
        "  FOR UPDATE SKIP LOCKED"
        ")"
        "UPDATE nsv2_name_work_items AS w"
        " SET status = 'running', worker_id = $2, started_at = NOW(), attempt_count = attempt_count + 1, error_message = ''"
        " WHERE w.id IN (SELECT id FROM candidate)"
        " RETURNING w.id, w.task_key, w.chains_csv, w.name_block_key, w.signature_prefix",
        2,
        nullptr,
        values,
        nullptr,
        nullptr,
        0
    );
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        std::string error = PQerrorMessage(conn);
        PQclear(result);
        PQexec(conn, "ROLLBACK");
        throw std::runtime_error(error);
    }

    Task task;
    if (PQntuples(result) > 0) {
        task.id = std::stoll(PQgetvalue(result, 0, 0));
        task.taskKey = PQgetvalue(result, 0, 1);
        task.chainsCsv = PQgetvalue(result, 0, 2);
        task.blockKey = PQgetvalue(result, 0, 3);
        task.signaturePrefix = PQgetvalue(result, 0, 4);
        task.valid = true;
    }
    PQclear(result);

    PGresult* end = PQexec(conn, task.valid ? "COMMIT" : "ROLLBACK");
    ensureOk(end, PGRES_COMMAND_OK, conn);
    PQclear(end);
    return task;
}

static std::vector<Atom> loadAtoms(PGconn* conn, const Settings& settings, const Task& task) {
    std::string prefixLen = std::to_string(task.signaturePrefix.size());
    const char* values[5] = {
        settings.runLabel.c_str(),
        task.chainsCsv.c_str(),
        task.blockKey.c_str(),
        prefixLen.c_str(),
        task.signaturePrefix.c_str(),
    };
    PGresult* result = PQexecParams(
        conn,
        "SELECT atom_id, chain, name_norm, contract_count, nft_count "
        "FROM nsv2_name_atoms "
        "WHERE run_label = $1 "
        "  AND chain = ANY(string_to_array($2, ',')) "
        "  AND name_block_key = $3 "
        "  AND ($5 = '' OR left(name_signature_hash, $4::int) = $5) "
        "ORDER BY atom_id",
        5,
        nullptr,
        values,
        nullptr,
        nullptr,
        0
    );
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        std::string error = PQerrorMessage(conn);
        PQclear(result);
        throw std::runtime_error(error);
    }
    std::vector<Atom> atoms;
    atoms.reserve(PQntuples(result));
    for (int row = 0; row < PQntuples(result); ++row) {
        Atom atom;
        atom.atomId = std::stoll(PQgetvalue(result, row, 0));
        atom.chain = PQgetvalue(result, row, 1);
        atom.name = PQgetvalue(result, row, 2);
        atom.contractCount = std::stoll(PQgetvalue(result, row, 3));
        atom.nftCount = std::stoll(PQgetvalue(result, row, 4));
        atom.trigrams = buildTrigrams(atom.name);
        atoms.push_back(std::move(atom));
    }
    PQclear(result);
    return atoms;
}

static std::vector<Edge> computeEdges(const std::vector<Atom>& atoms, const Settings& settings) {
    std::vector<Edge> edges;
    double minThreshold = *std::min_element(settings.thresholds.begin(), settings.thresholds.end());
    std::unordered_map<std::string, std::vector<int>> inverted;

    for (int i = 0; i < static_cast<int>(atoms.size()); ++i) {
        const Atom& left = atoms[i];
        std::unordered_map<int, int> sharedCounts;
        for (const auto& trigram : left.trigrams) {
            auto it = inverted.find(trigram);
            if (it == inverted.end()) {
                continue;
            }
            for (int j : it->second) {
                sharedCounts[j] += 1;
            }
        }

        for (const auto& entry : sharedCounts) {
            int j = entry.first;
            const Atom& right = atoms[j];
            if (std::abs(static_cast<int>(left.name.size()) - static_cast<int>(right.name.size())) > settings.maxLenDelta) {
                continue;
            }
            int shared = entry.second;
            double jaccard = trigramJaccard(left, right, shared);
            bool substringHit = left.name.find(right.name) != std::string::npos || right.name.find(left.name) != std::string::npos;
            if (!substringHit && jaccard < settings.trigramCutoff) {
                continue;
            }
            double similarity = levenshteinRatio(left.name, right.name);
            if (similarity < minThreshold) {
                continue;
            }
            edges.push_back(Edge{right.atomId, left.atomId, similarity});
        }

        for (const auto& trigram : left.trigrams) {
            inverted[trigram].push_back(i);
        }
    }
    return edges;
}

static std::string escapeLiteral(PGconn* conn, const std::string& value) {
    char* escaped = PQescapeLiteral(conn, value.c_str(), value.size());
    if (escaped == nullptr) {
        throw std::runtime_error(PQerrorMessage(conn));
    }
    std::string out(escaped);
    PQfreemem(escaped);
    return out;
}

static void insertEdges(PGconn* conn, const Settings& settings, const Task& task, const std::vector<Edge>& edges) {
    if (edges.empty()) {
        return;
    }
    std::string runLiteral = escapeLiteral(conn, settings.runLabel);
    const size_t batchSize = 500;
    for (size_t start = 0; start < edges.size(); start += batchSize) {
        size_t end = std::min(start + batchSize, edges.size());
        std::ostringstream sql;
        sql << "INSERT INTO nsv2_name_match_edges (run_label, task_id, left_atom_id, right_atom_id, similarity_score) VALUES ";
        bool first = true;
        for (size_t index = start; index < end; ++index) {
            const Edge& edge = edges[index];
            if (!first) {
                sql << ',';
            }
            first = false;
            sql << '(' << runLiteral << ',' << task.id << ',' << edge.leftAtomId << ',' << edge.rightAtomId << ',' << edge.similarity << ')';
        }
        sql << " ON CONFLICT (run_label, task_id, left_atom_id, right_atom_id) DO NOTHING";
        PGresult* result = PQexec(conn, sql.str().c_str());
        ensureOk(result, PGRES_COMMAND_OK, conn);
        PQclear(result);
    }
}

static void finishTask(PGconn* conn, const Task& task, long long edgeCount) {
    std::string taskId = std::to_string(task.id);
    std::string edgeCountStr = std::to_string(edgeCount);
    const char* values[2] = {edgeCountStr.c_str(), taskId.c_str()};
    PGresult* result = PQexecParams(
        conn,
        "UPDATE nsv2_name_work_items SET status = 'done', edge_count = $1::bigint, finished_at = NOW() WHERE id = $2::bigint",
        2,
        nullptr,
        values,
        nullptr,
        nullptr,
        0
    );
    ensureOk(result, PGRES_COMMAND_OK, conn);
    PQclear(result);
}

static void failTask(PGconn* conn, const Task& task, const std::string& error) {
    std::string taskId = std::to_string(task.id);
    const char* values[2] = {error.c_str(), taskId.c_str()};
    PGresult* result = PQexecParams(
        conn,
        "UPDATE nsv2_name_work_items SET status = 'failed', error_message = $1, finished_at = NOW() WHERE id = $2::bigint",
        2,
        nullptr,
        values,
        nullptr,
        nullptr,
        0
    );
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
        PQclear(result);
        return;
    }
    PQclear(result);
}

int main(int argc, char** argv) {
    try {
        Settings settings = parseArgs(argc, argv);
        PGconn* conn = connectDb();
        while (true) {
            Task task = claimTask(conn, settings);
            if (!task.valid) {
                break;
            }
            try {
                std::vector<Atom> atoms = loadAtoms(conn, settings, task);
                std::vector<Edge> edges = computeEdges(atoms, settings);
                insertEdges(conn, settings, task, edges);
                finishTask(conn, task, static_cast<long long>(edges.size()));
                std::cout << settings.workerId << " finished task " << task.taskKey << " edges=" << edges.size() << std::endl;
            } catch (const std::exception& ex) {
                failTask(conn, task, ex.what());
                std::cerr << settings.workerId << " failed task " << task.taskKey << ": " << ex.what() << std::endl;
            }
        }
        PQfinish(conn);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
}
