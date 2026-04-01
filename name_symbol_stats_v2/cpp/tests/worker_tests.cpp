#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "worker_core.hpp"

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << message << std::endl;
        std::exit(1);
    }
}

name_worker::Atom makeAtom(long long atomId, std::string chain, std::string name) {
    name_worker::Atom atom;
    atom.atomId = atomId;
    atom.chain = std::move(chain);
    atom.name = std::move(name);
    atom.contractCount = 1;
    atom.nftCount = 1;
    atom.trigrams = name_worker::buildTrigrams(atom.name);
    return atom;
}

void testClaimSqlReclaimsExpiredRunningTasks() {
    const std::string sql = name_worker::buildClaimTaskSql();
    require(sql.find("status = 'pending'") != std::string::npos, "claim SQL must select pending tasks");
    require(sql.find("status = 'running'") != std::string::npos, "claim SQL must consider running tasks");
    require(sql.find("make_interval(secs => $3::int)") != std::string::npos, "claim SQL must use lease timeout");
}

void testLongNameMatchesPriorShortName() {
    name_worker::Settings settings;
    settings.thresholds = {0.0};
    settings.trigramCutoff = 0.95;
    settings.maxLenDelta = 12;

    std::vector<name_worker::Atom> atoms;
    atoms.push_back(makeAtom(1, "ethereum", "a"));
    atoms.push_back(makeAtom(2, "base", "alpha"));

    std::vector<name_worker::Edge> emitted;
    const name_worker::EmitStats stats = name_worker::emitEdgesWithStats(atoms, settings, [&](const name_worker::Edge& edge) {
        emitted.push_back(edge);
    });

    require(stats.emittedEdges == 1, "expected one streamed edge when long name contains prior short name");
    require(emitted.size() == 1, "sink must receive one edge");
    require(emitted[0].leftAtomId == 1 && emitted[0].rightAtomId == 2, "edge atom ids do not match expected order");
}

void testShortNameMatchesPriorLongName() {
    name_worker::Settings settings;
    settings.thresholds = {0.0};
    settings.trigramCutoff = 0.95;
    settings.maxLenDelta = 12;

    std::vector<name_worker::Atom> atoms;
    atoms.push_back(makeAtom(1, "ethereum", "banana"));
    atoms.push_back(makeAtom(2, "base", "an"));

    std::vector<name_worker::Edge> emitted;
    const name_worker::EmitStats stats = name_worker::emitEdgesWithStats(atoms, settings, [&](const name_worker::Edge& edge) {
        emitted.push_back(edge);
    });

    require(stats.emittedEdges == 1, "expected one streamed edge when short name is contained in prior long name");
    require(emitted.size() == 1, "sink must receive one edge for reverse ordering");
    require(emitted[0].leftAtomId == 1 && emitted[0].rightAtomId == 2, "reverse-order substring edge atom ids do not match expected order");
}

void testLengthBucketsSkipOutOfRangePostingVisits() {
    name_worker::Settings settings;
    settings.thresholds = {0.0};
    settings.trigramCutoff = 0.10;
    settings.maxLenDelta = 2;

    std::vector<name_worker::Atom> atoms;
    atoms.push_back(makeAtom(1, "ethereum", "abcxxxxxxx"));
    atoms.push_back(makeAtom(2, "base", "abc"));

    const name_worker::EmitStats stats = name_worker::emitEdgesWithStats(atoms, settings, [](const name_worker::Edge&) {});
    require(stats.postingVisits == 0, "length buckets should avoid visiting postings outside the allowed length delta");
    require(stats.candidatePairs == 0, "out-of-range length candidates should not enter sharedCounts");
}

void testTrigramCountGateSkipsImpossibleNonSubstringCandidate() {
    name_worker::Settings settings;
    settings.thresholds = {0.0};
    settings.trigramCutoff = 0.80;
    settings.maxLenDelta = 64;

    std::vector<name_worker::Atom> atoms;
    atoms.push_back(makeAtom(1, "ethereum", "abcxyzuvw"));
    atoms.push_back(makeAtom(2, "base", "abcmmmmmmmmmmmmmmmmmmmmmmmmmmmm"));

    const name_worker::EmitStats stats = name_worker::emitEdgesWithStats(atoms, settings, [](const name_worker::Edge&) {});
    require(stats.postingVisits > 0, "test setup must exercise at least one posting list");
    require(stats.candidatePairs == 0, "impossible non-substring pairs should be rejected before sharedCounts growth");
}

}  // namespace

int main() {
    testClaimSqlReclaimsExpiredRunningTasks();
    testLongNameMatchesPriorShortName();
    testShortNameMatchesPriorLongName();
    testLengthBucketsSkipOutOfRangePostingVisits();
    testTrigramCountGateSkipsImpossibleNonSubstringCandidate();
    std::cout << "name_worker_tests passed" << std::endl;
    return 0;
}
