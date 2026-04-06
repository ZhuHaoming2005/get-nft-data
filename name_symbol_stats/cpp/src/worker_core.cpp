#include "worker_core.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace name_worker {
namespace {

constexpr int kLengthBucketSize = 4;
constexpr int kTrigramBucketSize = 4;

using TrigramPostingMap = std::unordered_map<std::string, std::vector<int>>;
using TrigramBucketMap = std::unordered_map<int, TrigramPostingMap>;
using LengthBucketMap = std::unordered_map<int, TrigramBucketMap>;
using ShortIndexMap = std::unordered_map<std::string, std::vector<int>>;

int lengthBucketFor(int length) {
    if (length <= 0) {
        return 0;
    }
    return (length / kLengthBucketSize) * kLengthBucketSize;
}

int trigramCountBucketFor(int trigramCount) {
    if (trigramCount <= 0) {
        return 0;
    }
    return (trigramCount / kTrigramBucketSize) * kTrigramBucketSize;
}

std::vector<int> collectLengthBucketsForRange(int nameLength, int maxLenDelta) {
    const int minLength = std::max(0, nameLength - maxLenDelta);
    const int maxLength = nameLength + maxLenDelta;
    std::vector<int> buckets;
    buckets.reserve(static_cast<std::size_t>((maxLength - minLength) / kLengthBucketSize + 2));
    for (int bucket = lengthBucketFor(minLength); bucket <= lengthBucketFor(maxLength); bucket += kLengthBucketSize) {
        buckets.push_back(bucket);
    }
    return buckets;
}

std::vector<int> collectTrigramBucketsForRange(int trigramCount, double trigramCutoff) {
    if (trigramCount <= 0 || trigramCutoff <= 0.0) {
        return {};
    }
    constexpr double epsilon = 1e-9;
    const int minCount = std::max(0, static_cast<int>(std::ceil(static_cast<double>(trigramCount) * trigramCutoff - epsilon)));
    const int maxCount = std::max(0, static_cast<int>(std::floor(static_cast<double>(trigramCount) / trigramCutoff + epsilon)));
    if (maxCount < minCount) {
        return {};
    }
    std::vector<int> buckets;
    buckets.reserve(static_cast<std::size_t>((trigramCountBucketFor(maxCount) - trigramCountBucketFor(minCount)) / kTrigramBucketSize + 1));
    for (int bucket = trigramCountBucketFor(minCount); bucket <= trigramCountBucketFor(maxCount); bucket += kTrigramBucketSize) {
        buckets.push_back(bucket);
    }
    return buckets;
}

std::vector<std::string> collectShortSubstrings(const std::string& value) {
    std::vector<std::string> out;
    if (value.empty()) {
        return out;
    }
    std::unordered_set<std::string> seen;
    seen.reserve(value.size() * 2);
    const std::size_t maxLen = std::min<std::size_t>(2, value.size());
    for (std::size_t len = 1; len <= maxLen; ++len) {
        for (std::size_t index = 0; index + len <= value.size(); ++index) {
            std::string token = value.substr(index, len);
            if (seen.insert(token).second) {
                out.push_back(std::move(token));
            }
        }
    }
    return out;
}

bool isSubstringHit(const Atom& left, const Atom& right) {
    return left.name.find(right.name) != std::string::npos || right.name.find(left.name) != std::string::npos;
}

double maxPossibleJaccard(int leftTrigramCount, int rightTrigramCount) {
    if (leftTrigramCount <= 0 || rightTrigramCount <= 0) {
        return 0.0;
    }
    const int shared = std::min(leftTrigramCount, rightTrigramCount);
    return static_cast<double>(shared) / static_cast<double>(leftTrigramCount + rightTrigramCount - shared);
}

bool passesCandidateBounds(
    const Atom& left,
    const Atom& right,
    int leftNameLength,
    int rightNameLength,
    int leftTrigramCount,
    int rightTrigramCount,
    const Settings& settings
) {
    if (std::abs(leftNameLength - rightNameLength) > settings.maxLenDelta) {
        return false;
    }
    if (isSubstringHit(left, right)) {
        return true;
    }
    return maxPossibleJaccard(leftTrigramCount, rightTrigramCount) >= settings.trigramCutoff;
}

void visitPostingMap(
    const TrigramPostingMap& postingMap,
    const std::vector<std::string>& queryTrigrams,
    const std::vector<Atom>& atoms,
    const std::vector<int>& nameLengths,
    const std::vector<int>& trigramCounts,
    const Atom& left,
    int leftNameLength,
    int leftTrigramCount,
    const Settings& settings,
    std::unordered_map<int, int>& sharedCounts,
    EmitStats& stats
) {
    for (const auto& trigram : queryTrigrams) {
        const auto postingIt = postingMap.find(trigram);
        if (postingIt == postingMap.end()) {
            continue;
        }
        stats.postingVisits += postingIt->second.size();
        for (int j : postingIt->second) {
            auto sharedIt = sharedCounts.find(j);
            if (sharedIt != sharedCounts.end()) {
                sharedIt->second += 1;
                continue;
            }
            const Atom& right = atoms[static_cast<std::size_t>(j)];
            const int rightNameLength = nameLengths[static_cast<std::size_t>(j)];
            const int rightTrigramCount = trigramCounts[static_cast<std::size_t>(j)];
            if (!passesCandidateBounds(left, right, leftNameLength, rightNameLength, leftTrigramCount, rightTrigramCount, settings)) {
                continue;
            }
            sharedCounts.emplace(j, 1);
            ++stats.candidatePairs;
        }
    }
}

}  // namespace

std::vector<std::string> split(const std::string& value, char sep) {
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

std::vector<double> parseThresholds(const std::string& value) {
    std::vector<double> out;
    for (const auto& part : split(value, ',')) {
        out.push_back(std::stod(part));
    }
    if (out.empty()) {
        throw std::runtime_error("thresholds cannot be empty");
    }
    return out;
}

std::vector<std::string> buildTrigrams(const std::string& value) {
    std::vector<std::string> out;
    if (value.empty()) {
        return out;
    }
    if (value.size() < 3) {
        out.push_back(value);
        return out;
    }
    out.reserve(value.size() - 2);
    for (std::size_t index = 0; index + 2 < value.size(); ++index) {
        out.push_back(value.substr(index, 3));
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

double levenshteinRatio(const std::string& left, const std::string& right) {
    if (left == right) {
        return 100.0;
    }
    if (left.empty() || right.empty()) {
        return 0.0;
    }
    std::vector<int> prev(right.size() + 1);
    std::vector<int> curr(right.size() + 1);
    for (std::size_t index = 0; index <= right.size(); ++index) {
        prev[index] = static_cast<int>(index);
    }
    for (std::size_t i = 0; i < left.size(); ++i) {
        curr[0] = static_cast<int>(i + 1);
        for (std::size_t j = 0; j < right.size(); ++j) {
            const int cost = left[i] == right[j] ? 0 : 1;
            curr[j + 1] = std::min({
                prev[j + 1] + 1,
                curr[j] + 1,
                prev[j] + cost,
            });
        }
        std::swap(prev, curr);
    }
    const int distance = prev[right.size()];
    const int maxLen = static_cast<int>(std::max(left.size(), right.size()));
    return 100.0 * (1.0 - static_cast<double>(distance) / static_cast<double>(maxLen));
}

double trigramJaccard(const Atom& left, const Atom& right, int shared) {
    const int unionSize = static_cast<int>(left.trigrams.size() + right.trigrams.size() - shared);
    if (unionSize <= 0) {
        return 0.0;
    }
    return static_cast<double>(shared) / static_cast<double>(unionSize);
}

const std::string& buildClaimTaskSql() {
    static const std::string sql =
        "WITH candidate AS ("
        "  SELECT id "
        "  FROM nsv2_name_work_items "
        "  WHERE run_label = $1 "
        "    AND ("
        "      status = 'pending' "
        "      OR (status = 'running' AND coalesce(started_at, to_timestamp(0)) < NOW() - make_interval(secs => $3::int))"
        "    ) "
        "  ORDER BY atom_count DESC, id "
        "  LIMIT 1 "
        "  FOR UPDATE SKIP LOCKED"
        ")"
        "UPDATE nsv2_name_work_items AS w "
        " SET status = 'running', worker_id = $2, started_at = NOW(), finished_at = NULL, attempt_count = attempt_count + 1, error_message = '' "
        " WHERE w.id IN (SELECT id FROM candidate) "
        " RETURNING w.id, w.task_key, w.chains_csv, w.name_block_key, w.signature_prefix";
    return sql;
}

EmitStats emitEdgesWithStats(const std::vector<Atom>& atoms, const Settings& settings, const std::function<void(const Edge&)>& sink) {
    if (settings.thresholds.empty()) {
        throw std::runtime_error("thresholds cannot be empty");
    }

    std::vector<int> nameLengths;
    std::vector<int> trigramCounts;
    std::vector<int> nameLengthBuckets;
    std::vector<int> trigramCountBuckets;
    nameLengths.reserve(atoms.size());
    trigramCounts.reserve(atoms.size());
    nameLengthBuckets.reserve(atoms.size());
    trigramCountBuckets.reserve(atoms.size());

    for (const auto& atom : atoms) {
        const int nameLength = static_cast<int>(atom.name.size());
        const int trigramCount = static_cast<int>(atom.trigrams.size());
        nameLengths.push_back(nameLength);
        trigramCounts.push_back(trigramCount);
        nameLengthBuckets.push_back(lengthBucketFor(nameLength));
        trigramCountBuckets.push_back(trigramCountBucketFor(trigramCount));
    }

    const double minThreshold = *std::min_element(settings.thresholds.begin(), settings.thresholds.end());
    LengthBucketMap postingIndex;
    postingIndex.reserve(atoms.size());
    ShortIndexMap shortNames;
    ShortIndexMap shortSubstringIndex;

    EmitStats stats;
    for (int i = 0; i < static_cast<int>(atoms.size()); ++i) {
        const Atom& left = atoms[static_cast<std::size_t>(i)];
        const int leftNameLength = nameLengths[static_cast<std::size_t>(i)];
        const int leftTrigramCount = trigramCounts[static_cast<std::size_t>(i)];
        const std::vector<int> candidateLengthBuckets = collectLengthBucketsForRange(leftNameLength, settings.maxLenDelta);
        const std::vector<int> candidateTrigramBuckets = collectTrigramBucketsForRange(leftTrigramCount, settings.trigramCutoff);
        const auto shortTokens = leftNameLength >= 3 ? collectShortSubstrings(left.name) : std::vector<std::string>{};

        std::unordered_map<int, int> sharedCounts;
        sharedCounts.reserve(left.trigrams.size() * 4 + shortTokens.size() * 4 + 8);

        for (int candidateLengthBucket : candidateLengthBuckets) {
            const auto lengthIt = postingIndex.find(candidateLengthBucket);
            if (lengthIt == postingIndex.end()) {
                continue;
            }
            const auto& trigramBuckets = lengthIt->second;
            if (candidateTrigramBuckets.empty()) {
                for (const auto& trigramBucketEntry : trigramBuckets) {
                    visitPostingMap(
                        trigramBucketEntry.second,
                        left.trigrams,
                        atoms,
                        nameLengths,
                        trigramCounts,
                        left,
                        leftNameLength,
                        leftTrigramCount,
                        settings,
                        sharedCounts,
                        stats
                    );
                }
                continue;
            }
            for (int candidateTrigramBucket : candidateTrigramBuckets) {
                const auto trigramBucketIt = trigramBuckets.find(candidateTrigramBucket);
                if (trigramBucketIt == trigramBuckets.end()) {
                    continue;
                }
                visitPostingMap(
                    trigramBucketIt->second,
                    left.trigrams,
                    atoms,
                    nameLengths,
                    trigramCounts,
                    left,
                    leftNameLength,
                    leftTrigramCount,
                    settings,
                    sharedCounts,
                    stats
                );
            }
        }

        if (leftNameLength < 3) {
            const auto substringIt = shortSubstringIndex.find(left.name);
            if (substringIt != shortSubstringIndex.end()) {
                for (int j : substringIt->second) {
                    if (sharedCounts.find(j) != sharedCounts.end()) {
                        continue;
                    }
                    const Atom& right = atoms[static_cast<std::size_t>(j)];
                    if (!passesCandidateBounds(left, right, leftNameLength, nameLengths[static_cast<std::size_t>(j)], leftTrigramCount, trigramCounts[static_cast<std::size_t>(j)], settings)) {
                        continue;
                    }
                    sharedCounts.emplace(j, 0);
                    ++stats.candidatePairs;
                }
            }
        } else {
            for (const auto& token : shortTokens) {
                const auto shortIt = shortNames.find(token);
                if (shortIt == shortNames.end()) {
                    continue;
                }
                for (int j : shortIt->second) {
                    if (sharedCounts.find(j) != sharedCounts.end()) {
                        continue;
                    }
                    const Atom& right = atoms[static_cast<std::size_t>(j)];
                    if (!passesCandidateBounds(left, right, leftNameLength, nameLengths[static_cast<std::size_t>(j)], leftTrigramCount, trigramCounts[static_cast<std::size_t>(j)], settings)) {
                        continue;
                    }
                    sharedCounts.emplace(j, 0);
                    ++stats.candidatePairs;
                }
            }
        }

        for (const auto& entry : sharedCounts) {
            const int j = entry.first;
            const Atom& right = atoms[static_cast<std::size_t>(j)];
            const bool substringHit = isSubstringHit(left, right);
            if (!substringHit) {
                const double jaccard = trigramJaccard(left, right, entry.second);
                if (jaccard < settings.trigramCutoff) {
                    continue;
                }
            }
            ++stats.scoredPairs;
            const double similarity = levenshteinRatio(left.name, right.name);
            if (similarity < minThreshold) {
                continue;
            }
            sink(Edge{right.atomId, left.atomId, similarity});
            ++stats.emittedEdges;
        }

        auto& postingMap = postingIndex[nameLengthBuckets[static_cast<std::size_t>(i)]][trigramCountBuckets[static_cast<std::size_t>(i)]];
        postingMap.reserve(std::max<std::size_t>(postingMap.size(), left.trigrams.size()));
        for (const auto& trigram : left.trigrams) {
            postingMap[trigram].push_back(i);
        }
        if (leftNameLength < 3) {
            shortNames[left.name].push_back(i);
        } else {
            for (const auto& token : shortTokens) {
                shortSubstringIndex[token].push_back(i);
            }
        }
    }

    return stats;
}

std::size_t emitEdges(const std::vector<Atom>& atoms, const Settings& settings, const std::function<void(const Edge&)>& sink) {
    return emitEdgesWithStats(atoms, settings, sink).emittedEdges;
}

}  // namespace name_worker


