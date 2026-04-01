#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace name_worker {

struct Settings {
    std::string runLabel;
    std::string workerId = "worker-1";
    double trigramCutoff = 0.35;
    int maxLenDelta = 12;
    std::vector<double> thresholds{85.0, 90.0, 95.0};
    int taskLeaseSeconds = 3600;
    std::size_t insertBatchSize = 500;
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

struct EmitStats {
    std::size_t emittedEdges = 0;
    std::size_t postingVisits = 0;
    std::size_t candidatePairs = 0;
    std::size_t scoredPairs = 0;
};

std::vector<std::string> split(const std::string& value, char sep);
std::vector<double> parseThresholds(const std::string& value);
std::vector<std::string> buildTrigrams(const std::string& value);
double levenshteinRatio(const std::string& left, const std::string& right);
double trigramJaccard(const Atom& left, const Atom& right, int shared);
const std::string& buildClaimTaskSql();
EmitStats emitEdgesWithStats(const std::vector<Atom>& atoms, const Settings& settings, const std::function<void(const Edge&)>& sink);
std::size_t emitEdges(const std::vector<Atom>& atoms, const Settings& settings, const std::function<void(const Edge&)>& sink);

}  // namespace name_worker
