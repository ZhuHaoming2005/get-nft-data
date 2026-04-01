#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "worker_core.hpp"

namespace nw = name_worker;

namespace {

using ConnPtr = std::unique_ptr<PGconn, decltype(&PQfinish)>;
using ResultPtr = std::unique_ptr<PGresult, decltype(&PQclear)>;

std::string envOr(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    return value ? value : fallback;
}

nw::Settings parseArgs(int argc, char** argv) {
    nw::Settings settings;
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        if (arg == "--run-label" && index + 1 < argc) {
            settings.runLabel = argv[++index];
        } else if (arg == "--worker-id" && index + 1 < argc) {
            settings.workerId = argv[++index];
        } else if (arg == "--trigram-cutoff" && index + 1 < argc) {
            settings.trigramCutoff = std::stod(argv[++index]);
        } else if (arg == "--max-len-delta" && index + 1 < argc) {
            settings.maxLenDelta = std::stoi(argv[++index]);
        } else if (arg == "--thresholds" && index + 1 < argc) {
            settings.thresholds = nw::parseThresholds(argv[++index]);
        } else if (arg == "--task-lease-seconds" && index + 1 < argc) {
            settings.taskLeaseSeconds = std::stoi(argv[++index]);
        } else if (arg == "--insert-batch-size" && index + 1 < argc) {
            settings.insertBatchSize = static_cast<std::size_t>(std::stoull(argv[++index]));
        }
    }
    if (settings.runLabel.empty()) {
        throw std::runtime_error("--run-label is required");
    }
    if (settings.thresholds.empty()) {
        throw std::runtime_error("thresholds cannot be empty");
    }
    if (settings.taskLeaseSeconds <= 0) {
        throw std::runtime_error("task lease must be positive");
    }
    if (settings.insertBatchSize == 0) {
        throw std::runtime_error("insert batch size must be positive");
    }
    return settings;
}

ResultPtr wrapResult(PGresult* raw, PGconn* conn) {
    if (raw == nullptr) {
        throw std::runtime_error(PQerrorMessage(conn));
    }
    return ResultPtr(raw, &PQclear);
}

void ensureOk(const ResultPtr& result, ExecStatusType expected, PGconn* conn) {
    if (PQresultStatus(result.get()) != expected) {
        throw std::runtime_error(PQerrorMessage(conn));
    }
}

ResultPtr exec(PGconn* conn, const char* sql) {
    return wrapResult(PQexec(conn, sql), conn);
}

ResultPtr execParams(PGconn* conn, const std::string& sql, int paramCount, const char* const* values) {
    return wrapResult(PQexecParams(conn, sql.c_str(), paramCount, nullptr, values, nullptr, nullptr, 0), conn);
}

void execCommand(PGconn* conn, const char* sql) {
    auto result = exec(conn, sql);
    ensureOk(result, PGRES_COMMAND_OK, conn);
}

void rollbackQuietly(PGconn* conn) {
    if (conn == nullptr) {
        return;
    }
    const auto txStatus = PQtransactionStatus(conn);
    if (txStatus != PQTRANS_INTRANS && txStatus != PQTRANS_INERROR) {
        return;
    }
    try {
        execCommand(conn, "ROLLBACK");
    } catch (...) {
    }
}

ConnPtr connectDb() {
    std::ostringstream dsn;
    dsn << "host=" << envOr("DB_HOST", "localhost")
        << " port=" << envOr("DB_PORT", "5432")
        << " dbname=" << envOr("DB_NAME", "nft_data")
        << " user=" << envOr("DB_USER", "postgres")
        << " password=" << envOr("DB_PASS", "123456");
    ConnPtr conn(PQconnectdb(dsn.str().c_str()), &PQfinish);
    if (conn == nullptr || PQstatus(conn.get()) != CONNECTION_OK) {
        throw std::runtime_error(conn ? PQerrorMessage(conn.get()) : "PQconnectdb returned null");
    }
    return conn;
}

nw::Task claimTask(PGconn* conn, const nw::Settings& settings) {
    execCommand(conn, "BEGIN");

    try {
        const std::string leaseSeconds = std::to_string(settings.taskLeaseSeconds);
        const char* values[3] = {
            settings.runLabel.c_str(),
            settings.workerId.c_str(),
            leaseSeconds.c_str(),
        };
        auto result = execParams(conn, nw::buildClaimTaskSql(), 3, values);
        ensureOk(result, PGRES_TUPLES_OK, conn);

        nw::Task task;
        if (PQntuples(result.get()) > 0) {
            task.id = std::stoll(PQgetvalue(result.get(), 0, 0));
            task.taskKey = PQgetvalue(result.get(), 0, 1);
            task.chainsCsv = PQgetvalue(result.get(), 0, 2);
            task.blockKey = PQgetvalue(result.get(), 0, 3);
            task.signaturePrefix = PQgetvalue(result.get(), 0, 4);
            task.valid = true;
        }

        execCommand(conn, task.valid ? "COMMIT" : "ROLLBACK");
        return task;
    } catch (...) {
        rollbackQuietly(conn);
        throw;
    }
}

std::vector<nw::Atom> loadAtoms(PGconn* conn, const nw::Settings& settings, const nw::Task& task) {
    const std::string prefixLen = std::to_string(task.signaturePrefix.size());
    const char* values[5] = {
        settings.runLabel.c_str(),
        task.chainsCsv.c_str(),
        task.blockKey.c_str(),
        prefixLen.c_str(),
        task.signaturePrefix.c_str(),
    };
    auto result = execParams(
        conn,
        "SELECT atom_id, chain, name_norm, contract_count, nft_count "
        "FROM nsv2_name_atoms "
        "WHERE run_label = $1 "
        "  AND chain = ANY(string_to_array($2, ',')) "
        "  AND name_block_key = $3 "
        "  AND ($5 = '' OR left(name_signature_hash, $4::int) = $5) "
        "ORDER BY atom_id",
        5,
        values
    );
    ensureOk(result, PGRES_TUPLES_OK, conn);

    std::vector<nw::Atom> atoms;
    atoms.reserve(static_cast<std::size_t>(PQntuples(result.get())));
    for (int row = 0; row < PQntuples(result.get()); ++row) {
        nw::Atom atom;
        atom.atomId = std::stoll(PQgetvalue(result.get(), row, 0));
        atom.chain = PQgetvalue(result.get(), row, 1);
        atom.name = PQgetvalue(result.get(), row, 2);
        atom.contractCount = std::stoll(PQgetvalue(result.get(), row, 3));
        atom.nftCount = std::stoll(PQgetvalue(result.get(), row, 4));
        atom.trigrams = nw::buildTrigrams(atom.name);
        atoms.push_back(std::move(atom));
    }
    return atoms;
}

std::string buildInsertEdgesSql(std::size_t rowCount) {
    std::ostringstream sql;
    sql << "INSERT INTO nsv2_name_match_edges (run_label, task_id, left_atom_id, right_atom_id, similarity_score) VALUES ";
    int paramIndex = 3;
    for (std::size_t row = 0; row < rowCount; ++row) {
        if (row != 0) {
            sql << ',';
        }
        sql << "($1, $2::bigint, $" << paramIndex++ << "::bigint, $" << paramIndex++ << "::bigint, $" << paramIndex++ << "::double precision)";
    }
    sql << " ON CONFLICT (run_label, task_id, left_atom_id, right_atom_id) DO NOTHING";
    return sql.str();
}

void insertEdgeBatch(PGconn* conn, const nw::Settings& settings, const nw::Task& task, const std::vector<nw::Edge>& batch) {
    if (batch.empty()) {
        return;
    }

    std::vector<std::string> storage;
    storage.reserve(2 + batch.size() * 3);
    storage.push_back(settings.runLabel);
    storage.push_back(std::to_string(task.id));
    for (const auto& edge : batch) {
        storage.push_back(std::to_string(edge.leftAtomId));
        storage.push_back(std::to_string(edge.rightAtomId));
        storage.push_back(std::to_string(edge.similarity));
    }

    std::vector<const char*> values;
    values.reserve(storage.size());
    for (const auto& item : storage) {
        values.push_back(item.c_str());
    }

    auto result = execParams(conn, buildInsertEdgesSql(batch.size()), static_cast<int>(values.size()), values.data());
    ensureOk(result, PGRES_COMMAND_OK, conn);
}

class EdgeBatchInserter {
public:
    EdgeBatchInserter(PGconn* conn, const nw::Settings& settings, const nw::Task& task)
        : conn_(conn), settings_(settings), task_(task), batchSize_(std::max<std::size_t>(1, settings.insertBatchSize)) {
        batch_.reserve(batchSize_);
    }

    void push(const nw::Edge& edge) {
        batch_.push_back(edge);
        if (batch_.size() >= batchSize_) {
            flush();
        }
    }

    void flush() {
        insertEdgeBatch(conn_, settings_, task_, batch_);
        batch_.clear();
    }

private:
    PGconn* conn_;
    const nw::Settings& settings_;
    const nw::Task& task_;
    std::size_t batchSize_;
    std::vector<nw::Edge> batch_;
};

void cleanupTaskEdges(PGconn* conn, const nw::Settings& settings, const nw::Task& task) {
    const std::string taskId = std::to_string(task.id);
    const char* values[2] = {settings.runLabel.c_str(), taskId.c_str()};
    auto result = execParams(
        conn,
        "DELETE FROM nsv2_name_match_edges WHERE run_label = $1 AND task_id = $2::bigint",
        2,
        values
    );
    ensureOk(result, PGRES_COMMAND_OK, conn);
}

void finishTask(PGconn* conn, const nw::Task& task, long long edgeCount) {
    const std::string taskId = std::to_string(task.id);
    const std::string edgeCountStr = std::to_string(edgeCount);
    const char* values[2] = {edgeCountStr.c_str(), taskId.c_str()};
    auto result = execParams(
        conn,
        "UPDATE nsv2_name_work_items "
        "SET status = 'done', edge_count = $1::bigint, finished_at = NOW() "
        "WHERE id = $2::bigint",
        2,
        values
    );
    ensureOk(result, PGRES_COMMAND_OK, conn);
}

void failTask(PGconn* conn, const nw::Task& task, const std::string& error) {
    const std::string taskId = std::to_string(task.id);
    const char* values[2] = {error.c_str(), taskId.c_str()};
    auto result = execParams(
        conn,
        "UPDATE nsv2_name_work_items "
        "SET status = 'failed', error_message = $1, finished_at = NOW() "
        "WHERE id = $2::bigint",
        2,
        values
    );
    ensureOk(result, PGRES_COMMAND_OK, conn);
}

void cleanupAndFailTask(PGconn* conn, const nw::Settings& settings, const nw::Task& task, const std::string& error) {
    rollbackQuietly(conn);
    execCommand(conn, "BEGIN");
    try {
        cleanupTaskEdges(conn, settings, task);
        failTask(conn, task, error);
        execCommand(conn, "COMMIT");
    } catch (...) {
        rollbackQuietly(conn);
        throw;
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const nw::Settings settings = parseArgs(argc, argv);
        auto conn = connectDb();
        while (true) {
            const nw::Task task = claimTask(conn.get(), settings);
            if (!task.valid) {
                break;
            }
            try {
                cleanupTaskEdges(conn.get(), settings, task);
                const std::vector<nw::Atom> atoms = loadAtoms(conn.get(), settings, task);
                EdgeBatchInserter inserter(conn.get(), settings, task);
                const nw::EmitStats stats = nw::emitEdgesWithStats(atoms, settings, [&](const nw::Edge& edge) {
                    inserter.push(edge);
                });
                inserter.flush();
                finishTask(conn.get(), task, static_cast<long long>(stats.emittedEdges));
                std::cout
                    << settings.workerId
                    << " finished task " << task.taskKey
                    << " atoms=" << atoms.size()
                    << " edges=" << stats.emittedEdges
                    << " posting_visits=" << stats.postingVisits
                    << " candidates=" << stats.candidatePairs
                    << " scored=" << stats.scoredPairs
                    << std::endl;
            } catch (const std::exception& ex) {
                try {
                    cleanupAndFailTask(conn.get(), settings, task, ex.what());
                } catch (const std::exception& failEx) {
                    std::cerr << settings.workerId << " failed to clean up task " << task.taskKey << ": " << failEx.what() << std::endl;
                    throw;
                }
                std::cerr << settings.workerId << " failed task " << task.taskKey << ": " << ex.what() << std::endl;
            }
        }
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
}
