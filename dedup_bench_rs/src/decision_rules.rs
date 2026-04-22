#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DuplicateScoreRule {
    pub algorithm_id: &'static str,
    pub threshold: f64,
    pub description: &'static str,
}

const ORDINARY_RULES: &[DuplicateScoreRule] = &[
    DuplicateScoreRule {
        algorithm_id: "name_exact_normalized",
        threshold: 100.0,
        description: "score >= 100.0",
    },
    DuplicateScoreRule {
        algorithm_id: "name_jaro_winkler",
        threshold: 95.0,
        description: "score >= 95.0",
    },
    DuplicateScoreRule {
        algorithm_id: "name_damerau_levenshtein",
        threshold: 80.0,
        description: "score >= 80.0",
    },
    DuplicateScoreRule {
        algorithm_id: "name_monge_elkan",
        threshold: 85.0,
        description: "score >= 85.0",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_bm25",
        threshold: 0.60,
        description: "score >= 0.60",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_token_cosine",
        threshold: 0.80,
        description: "score >= 0.80",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_soft_tfidf",
        threshold: 0.75,
        description: "score >= 0.75",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_weighted_jaccard",
        threshold: 0.70,
        description: "score >= 0.70",
    },
];

#[cfg(test)]
pub fn ordinary_duplicate_score_rules() -> &'static [DuplicateScoreRule] {
    ORDINARY_RULES
}

pub fn duplicate_score_rule(algorithm_id: &str) -> Result<DuplicateScoreRule, String> {
    ORDINARY_RULES
        .iter()
        .copied()
        .find(|rule| rule.algorithm_id == algorithm_id)
        .ok_or_else(|| format!("missing duplicate rule for algorithm_id={algorithm_id}"))
}

#[cfg(test)]
fn threshold_text(threshold: f64) -> String {
    if (threshold - threshold.trunc()).abs() < f64::EPSILON {
        format!("{threshold:.1}")
    } else {
        format!("{threshold:.2}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn ordinary_rule_table_is_complete_and_consistent() {
        let expected = [
            DuplicateScoreRule {
                algorithm_id: "name_exact_normalized",
                threshold: 100.0,
                description: "score >= 100.0",
            },
            DuplicateScoreRule {
                algorithm_id: "name_jaro_winkler",
                threshold: 95.0,
                description: "score >= 95.0",
            },
            DuplicateScoreRule {
                algorithm_id: "name_damerau_levenshtein",
                threshold: 80.0,
                description: "score >= 80.0",
            },
            DuplicateScoreRule {
                algorithm_id: "name_monge_elkan",
                threshold: 85.0,
                description: "score >= 85.0",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_bm25",
                threshold: 0.60,
                description: "score >= 0.60",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_token_cosine",
                threshold: 0.80,
                description: "score >= 0.80",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_soft_tfidf",
                threshold: 0.75,
                description: "score >= 0.75",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_weighted_jaccard",
                threshold: 0.70,
                description: "score >= 0.70",
            },
        ];

        let actual = ordinary_duplicate_score_rules();
        assert_eq!(actual, expected.as_slice());
        assert_eq!(actual.len(), 8);

        let mut algorithm_ids = HashSet::new();
        for rule in actual {
            assert!(
                algorithm_ids.insert(rule.algorithm_id),
                "duplicate algorithm_id found: {}",
                rule.algorithm_id
            );
            assert_eq!(
                rule.description,
                format!("score >= {}", threshold_text(rule.threshold))
            );
            assert_eq!(duplicate_score_rule(rule.algorithm_id).unwrap(), *rule);
        }
    }

    #[test]
    fn lookup_errors_are_stable() {
        let err = duplicate_score_rule("missing_algorithm").unwrap_err();
        assert!(err.contains("missing duplicate rule"));
    }
}
