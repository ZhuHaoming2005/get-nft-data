fn normalize_nfkc(raw: &str) -> String {
    raw.trim().to_string()
}

fn collapse_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_matching_suffix(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let lower = trimmed.to_lowercase();

    for marker in ["#", "-", ":", "/"] {
        if let Some((head, tail)) = trimmed.rsplit_once(marker) {
            let tail = tail.trim();
            if !tail.is_empty() && tail.chars().all(|ch| ch.is_ascii_hexdigit() || ch == 'x' || ch == 'X') {
                return Some(head.trim().to_string());
            }
        }
    }

    if let Some(head) = trimmed.strip_suffix(')') {
        if let Some((head, tail)) = head.rsplit_once('(') {
            if tail.trim().chars().all(|ch| ch.is_ascii_digit()) {
                return Some(head.trim().to_string());
            }
        }
    }

    if let Some(head) = trimmed.strip_suffix(']') {
        if let Some((head, tail)) = head.rsplit_once('[') {
            if tail.trim().chars().all(|ch| ch.is_ascii_digit()) {
                return Some(head.trim().to_string());
            }
        }
    }

    if let Some((head, tail)) = trimmed.rsplit_once(' ') {
        let tail_lower = tail.to_lowercase();
        if tail.chars().all(|ch| ch.is_ascii_digit()) && (1..=12).contains(&tail.len()) {
            return Some(head.trim().to_string());
        }
        if tail_lower.starts_with("no.") || tail_lower.starts_with("no") || tail_lower.starts_with("nr.") || tail_lower.starts_with("nr") {
            let number = tail
                .trim_start_matches(|ch: char| ch.is_ascii_alphabetic() || ch == '.')
                .trim();
            if !number.is_empty() && number.chars().all(|ch| ch.is_ascii_digit()) {
                return Some(head.trim().to_string());
            }
        }
    }

    if lower == trimmed {
        None
    } else {
        None
    }
}

pub fn strip_trailing_number_suffix(raw: &str) -> String {
    let mut text = collapse_whitespace(&normalize_nfkc(raw));
    let mut guard = 0;
    while guard < 20 {
        guard += 1;
        let Some(updated) = strip_matching_suffix(&text) else {
            break;
        };
        if updated == text {
            break;
        }
        text = collapse_whitespace(&updated);
    }
    text
}

pub fn normalize_name(raw: &str) -> String {
    strip_trailing_number_suffix(raw).to_lowercase()
}

pub fn normalize_symbol(raw: &str) -> String {
    normalize_nfkc(raw).trim().to_lowercase()
}

pub fn normalize_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_lowercase()
}

pub fn normalize_text(raw: &str) -> String {
    collapse_whitespace(&normalize_nfkc(raw)).to_lowercase()
}
