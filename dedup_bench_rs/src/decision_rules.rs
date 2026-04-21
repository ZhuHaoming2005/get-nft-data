#![allow(dead_code)]

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DuplicateScoreRule {
    pub algorithm_id: &'static str,
    pub threshold: f64,
    pub description: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReferenceDuplicateRule {
    pub name_threshold: f64,
    pub metadata_threshold: f64,
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
        algorithm_id: "name_normalized_levenshtein",
        threshold: 80.0,
        description: "score >= 80.0",
    },
    DuplicateScoreRule {
        algorithm_id: "name_trigram_jaccard",
        threshold: 80.0,
        description: "score >= 80.0",
    },
    DuplicateScoreRule {
        algorithm_id: "name_current_hybrid",
        threshold: 90.0,
        description: "score >= 90.0",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_token_jaccard",
        threshold: 0.80,
        description: "score >= 0.80",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_jaro_winkler_doc",
        threshold: 0.90,
        description: "score >= 0.90",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_trigram_jaccard_doc",
        threshold: 0.75,
        description: "score >= 0.75",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_token_cosine",
        threshold: 0.80,
        description: "score >= 0.80",
    },
    DuplicateScoreRule {
        algorithm_id: "metadata_current_hybrid",
        threshold: 0.55,
        description: "score >= 0.55",
    },
];

const REFERENCE_RULE: ReferenceDuplicateRule = ReferenceDuplicateRule {
    name_threshold: 95.0,
    metadata_threshold: 0.55,
    description: "name_score >= 95.0 OR metadata_score >= 0.55",
};

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

pub fn reference_duplicate_rule() -> ReferenceDuplicateRule {
    REFERENCE_RULE
}

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
                algorithm_id: "name_normalized_levenshtein",
                threshold: 80.0,
                description: "score >= 80.0",
            },
            DuplicateScoreRule {
                algorithm_id: "name_trigram_jaccard",
                threshold: 80.0,
                description: "score >= 80.0",
            },
            DuplicateScoreRule {
                algorithm_id: "name_current_hybrid",
                threshold: 90.0,
                description: "score >= 90.0",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_token_jaccard",
                threshold: 0.80,
                description: "score >= 0.80",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_jaro_winkler_doc",
                threshold: 0.90,
                description: "score >= 0.90",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_trigram_jaccard_doc",
                threshold: 0.75,
                description: "score >= 0.75",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_token_cosine",
                threshold: 0.80,
                description: "score >= 0.80",
            },
            DuplicateScoreRule {
                algorithm_id: "metadata_current_hybrid",
                threshold: 0.55,
                description: "score >= 0.55",
            },
        ];

        let actual = ordinary_duplicate_score_rules();
        assert_eq!(actual, expected.as_slice());
        assert_eq!(actual.len(), 10);

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
    fn lookup_errors_and_reference_thresholds_are_stable() {
        let err = duplicate_score_rule("missing_algorithm").unwrap_err();
        assert!(err.contains("missing duplicate rule"));

        let rule = reference_duplicate_rule();
        assert_eq!(rule.name_threshold, 95.0);
        assert_eq!(rule.metadata_threshold, 0.55);
        assert_eq!(rule.description, "name_score >= 95.0 OR metadata_score >= 0.55");
    }
}
