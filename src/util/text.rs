/// Borrow at most `max` Unicode scalar values without scanning the full input.
/// Short strings and ASCII prefixes avoid character decoding entirely.
pub(crate) fn char_prefix(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    if text.as_bytes()[..max].is_ascii() {
        return &text[..max];
    }
    text.char_indices()
        .nth(max)
        .map_or(text, |(end, _)| &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn char_prefix_preserves_utf8_and_character_limits() {
        for text in ["", "ascii", "汉字🙂ab", "a🙂bc", "e\u{301}x", "\0🙂x"] {
            for limit in (0..=text.len() + 1).chain([usize::MAX]) {
                let expected: String = text.chars().take(limit).collect();
                let actual = char_prefix(text, limit);
                assert_eq!(actual, expected, "text={text:?}, limit={limit}");
                assert_eq!(actual.as_ptr(), text.as_ptr());
            }
        }
        for prefix in ["a".repeat(1024), "汉🙂".repeat(512)] {
            let text = format!("{prefix}{}", "长尾🙂".repeat(100_000));
            assert_eq!(char_prefix(&text, 1024), prefix);
        }
    }
}
