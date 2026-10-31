#[path = "charset_uca9.rs"]
mod uca9;

use std::borrow::Cow;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::OnceLock;

use encoding_rs::Encoding;
use unicode_normalization::char::{decompose_compatible, is_combining_mark};
use unicode_normalization::{is_nfkd_quick, IsNormalized, UnicodeNormalization};

use crate::mysql::errors as mysql_errors;
use crate::types::{QueryError, Value};

pub const DEFAULT_CHARSET: &str = "utf8mb4";
pub const DEFAULT_COLLATION: &str = "utf8mb4_0900_ai_ci";
pub const BINARY_COLLATION_ID: u16 = 63;
pub const UTF8MB4_0900_AI_CI_ID: u16 = 255;
// Audited against every packed DUCET entry; Hangul decomposition and
// implicit weights also remain within this UTF-8 byte expansion bound.
#[cfg(test)]
pub(crate) const UCA9_PRIMARY_MAX_EXPANSION: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CharsetInfo {
    pub name: &'static str,
    pub description: &'static str,
    pub default_collation: &'static str,
    pub max_len: u8,
    pub client_allowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollationInfo {
    pub name: &'static str,
    pub charset: &'static str,
    pub id: u16,
    pub is_default: bool,
    pub is_compiled: bool,
    pub sort_len: u8,
    pub pad_attribute: &'static str,
    pub case_sensitive: bool,
    pub accent_sensitive: bool,
    pub binary: bool,
    /// Precomputed `name.contains("_0900_")` for UCA 9.0's ignorable-NUL
    /// rules.  String comparison is an O(n log n) sort inner loop, so even a
    /// short substring scan belongs in collation initialization, not here.
    pub uca_0900: bool,
    /// MySQL's German phonebook collations expand umlauts and sharp-s before
    /// ordinary case/accent processing. Kept as metadata because LIKE can
    /// consult it once per input character on large scans.
    pub german_phonebook: bool,
    /// Whether the collation applies the small UCA compatibility-expansion
    /// table after accent folding. This used to be rediscovered with several
    /// substring searches for every non-ASCII character comparison.
    pub common_equivalences: bool,
    /// Precomputed `pad_attribute.eq_ignore_ascii_case("PAD SPACE")`.
    /// Avoids the per-row string compare that `trim_for_pad` would do
    /// on every `ORDER BY` / `GROUP BY` / equality compare; sysbench's
    /// sort path alone runs millions of compares per second.
    pub pad_space: bool,
}

#[inline]
pub fn compute_pad_space(pad_attribute: &str) -> bool {
    pad_attribute.eq_ignore_ascii_case("PAD SPACE")
}

macro_rules! charset {
    ($name:literal, $description:literal, $default:expr, $max_len:literal, $client_allowed:literal) => {
        CharsetInfo {
            name: $name,
            description: $description,
            default_collation: $default,
            max_len: $max_len,
            client_allowed: $client_allowed,
        }
    };
}

pub const CHARSETS: &[CharsetInfo] = &[
    charset!(
        "armscii8",
        "ARMSCII-8 Armenian",
        "armscii8_general_ci",
        1,
        true
    ),
    charset!("ascii", "US ASCII", "ascii_general_ci", 1, true),
    charset!(
        "big5",
        "Big5 Traditional Chinese",
        "big5_chinese_ci",
        2,
        true
    ),
    charset!("binary", "Binary pseudo charset", "binary", 1, true),
    charset!(
        "cp1250",
        "Windows Central European",
        "cp1250_general_ci",
        1,
        true
    ),
    charset!("cp1251", "Windows Cyrillic", "cp1251_general_ci", 1, true),
    charset!("cp1256", "Windows Arabic", "cp1256_general_ci", 1, true),
    charset!("cp1257", "Windows Baltic", "cp1257_general_ci", 1, true),
    charset!("cp850", "DOS West European", "cp850_general_ci", 1, true),
    charset!("cp852", "DOS Central European", "cp852_general_ci", 1, true),
    charset!("cp866", "DOS Russian", "cp866_general_ci", 1, true),
    charset!(
        "cp932",
        "SJIS for Windows Japanese",
        "cp932_japanese_ci",
        2,
        true
    ),
    charset!("dec8", "DEC West European", "dec8_swedish_ci", 1, true),
    charset!(
        "eucjpms",
        "UJIS for Windows Japanese",
        "eucjpms_japanese_ci",
        3,
        true
    ),
    charset!("euckr", "EUC-KR Korean", "euckr_korean_ci", 2, true),
    charset!(
        "gb18030",
        "China National Standard GB18030",
        "gb18030_chinese_ci",
        4,
        true
    ),
    charset!(
        "gb2312",
        "GB2312 Simplified Chinese",
        "gb2312_chinese_ci",
        2,
        true
    ),
    charset!("gbk", "GBK Simplified Chinese", "gbk_chinese_ci", 2, true),
    charset!("geostd8", "GEOSTD8 Georgian", "geostd8_general_ci", 1, true),
    charset!("greek", "ISO 8859-7 Greek", "greek_general_ci", 1, true),
    charset!("hebrew", "ISO 8859-8 Hebrew", "hebrew_general_ci", 1, true),
    charset!("hp8", "HP West European", "hp8_english_ci", 1, true),
    charset!(
        "keybcs2",
        "DOS Kamenicky Czech-Slovak",
        "keybcs2_general_ci",
        1,
        true
    ),
    charset!(
        "koi8r",
        "KOI8-R Relcom Russian",
        "koi8r_general_ci",
        1,
        true
    ),
    charset!("koi8u", "KOI8-U Ukrainian", "koi8u_general_ci", 1, true),
    charset!(
        "latin1",
        "cp1252 West European",
        "latin1_swedish_ci",
        1,
        true
    ),
    charset!(
        "latin2",
        "ISO 8859-2 Central European",
        "latin2_general_ci",
        1,
        true
    ),
    charset!("latin5", "ISO 8859-9 Turkish", "latin5_turkish_ci", 1, true),
    charset!("latin7", "ISO 8859-13 Baltic", "latin7_general_ci", 1, true),
    charset!("macce", "Mac Central European", "macce_general_ci", 1, true),
    charset!(
        "macroman",
        "Mac West European",
        "macroman_general_ci",
        1,
        true
    ),
    charset!("sjis", "Shift-JIS Japanese", "sjis_japanese_ci", 2, true),
    charset!("swe7", "7bit Swedish", "swe7_swedish_ci", 1, true),
    charset!("tis620", "TIS620 Thai", "tis620_thai_ci", 1, true),
    charset!("ucs2", "UCS-2 Unicode", "ucs2_general_ci", 2, false),
    charset!("ujis", "EUC-JP Japanese", "ujis_japanese_ci", 3, true),
    charset!("utf16", "UTF-16 Unicode", "utf16_general_ci", 4, false),
    charset!(
        "utf16le",
        "UTF-16LE Unicode",
        "utf16le_general_ci",
        4,
        false
    ),
    charset!("utf32", "UTF-32 Unicode", "utf32_general_ci", 4, false),
    charset!("utf8mb3", "UTF-8 Unicode", "utf8mb3_general_ci", 3, true),
    charset!("utf8mb4", "UTF-8 Unicode", DEFAULT_COLLATION, 4, true),
];

#[derive(Debug, Clone, Copy)]
struct CollationSeed {
    name: &'static str,
    charset: &'static str,
    id: u16,
    binary: bool,
    case_sensitive: bool,
    accent_sensitive: bool,
    sort_len: u8,
    pad_attribute: &'static str,
}

macro_rules! coll {
    ($name:literal, $charset:literal, $id:literal) => {
        CollationSeed {
            name: $name,
            charset: $charset,
            id: $id,
            binary: false,
            case_sensitive: false,
            accent_sensitive: false,
            sort_len: 1,
            pad_attribute: "PAD SPACE",
        }
    };
    ($name:literal, $charset:literal, $id:literal, bin) => {
        CollationSeed {
            name: $name,
            charset: $charset,
            id: $id,
            binary: true,
            case_sensitive: true,
            accent_sensitive: true,
            sort_len: 1,
            pad_attribute: "PAD SPACE",
        }
    };
    ($name:literal, $charset:literal, $id:literal, cs) => {
        CollationSeed {
            name: $name,
            charset: $charset,
            id: $id,
            binary: false,
            case_sensitive: true,
            accent_sensitive: true,
            sort_len: 1,
            pad_attribute: "PAD SPACE",
        }
    };
    ($name:literal, $charset:literal, $id:literal, no_pad_bin) => {
        CollationSeed {
            name: $name,
            charset: $charset,
            id: $id,
            binary: true,
            case_sensitive: true,
            accent_sensitive: true,
            sort_len: 0,
            pad_attribute: "NO PAD",
        }
    };
}

static BASE_COLLATIONS: &[CollationSeed] = &[
    coll!("big5_chinese_ci", "big5", 1),
    coll!("latin2_czech_cs", "latin2", 2, cs),
    coll!("dec8_swedish_ci", "dec8", 3),
    coll!("cp850_general_ci", "cp850", 4),
    coll!("latin1_german1_ci", "latin1", 5),
    coll!("hp8_english_ci", "hp8", 6),
    coll!("koi8r_general_ci", "koi8r", 7),
    coll!("latin1_swedish_ci", "latin1", 8),
    coll!("latin2_general_ci", "latin2", 9),
    coll!("swe7_swedish_ci", "swe7", 10),
    coll!("ascii_general_ci", "ascii", 11),
    coll!("ujis_japanese_ci", "ujis", 12),
    coll!("sjis_japanese_ci", "sjis", 13),
    coll!("cp1251_bulgarian_ci", "cp1251", 14),
    coll!("latin1_danish_ci", "latin1", 15),
    coll!("hebrew_general_ci", "hebrew", 16),
    coll!("tis620_thai_ci", "tis620", 18),
    coll!("euckr_korean_ci", "euckr", 19),
    coll!("latin7_estonian_cs", "latin7", 20, cs),
    coll!("latin2_hungarian_ci", "latin2", 21),
    coll!("koi8u_general_ci", "koi8u", 22),
    coll!("cp1251_ukrainian_ci", "cp1251", 23),
    coll!("gb2312_chinese_ci", "gb2312", 24),
    coll!("greek_general_ci", "greek", 25),
    coll!("cp1250_general_ci", "cp1250", 26),
    coll!("latin2_croatian_ci", "latin2", 27),
    coll!("gbk_chinese_ci", "gbk", 28),
    coll!("cp1257_lithuanian_ci", "cp1257", 29),
    coll!("latin5_turkish_ci", "latin5", 30),
    coll!("latin1_german2_ci", "latin1", 31),
    coll!("armscii8_general_ci", "armscii8", 32),
    coll!("utf8mb3_general_ci", "utf8mb3", 33),
    coll!("cp1250_czech_cs", "cp1250", 34, cs),
    coll!("ucs2_general_ci", "ucs2", 35),
    coll!("cp866_general_ci", "cp866", 36),
    coll!("keybcs2_general_ci", "keybcs2", 37),
    coll!("macce_general_ci", "macce", 38),
    coll!("macroman_general_ci", "macroman", 39),
    coll!("cp852_general_ci", "cp852", 40),
    coll!("latin7_general_ci", "latin7", 41),
    coll!("latin7_general_cs", "latin7", 42, cs),
    coll!("macce_bin", "macce", 43, bin),
    coll!("cp1250_croatian_ci", "cp1250", 44),
    coll!("utf8mb4_general_ci", "utf8mb4", 45),
    coll!("utf8mb4_bin", "utf8mb4", 46, bin),
    coll!("latin1_bin", "latin1", 47, bin),
    coll!("latin1_general_ci", "latin1", 48),
    coll!("latin1_general_cs", "latin1", 49, cs),
    coll!("cp1251_bin", "cp1251", 50, bin),
    coll!("cp1251_general_ci", "cp1251", 51),
    coll!("cp1251_general_cs", "cp1251", 52, cs),
    coll!("macroman_bin", "macroman", 53, bin),
    coll!("utf16_general_ci", "utf16", 54),
    coll!("utf16_bin", "utf16", 55, bin),
    coll!("utf16le_general_ci", "utf16le", 56),
    coll!("cp1256_general_ci", "cp1256", 57),
    coll!("cp1257_bin", "cp1257", 58, bin),
    coll!("cp1257_general_ci", "cp1257", 59),
    coll!("utf32_general_ci", "utf32", 60),
    coll!("utf32_bin", "utf32", 61, bin),
    coll!("utf16le_bin", "utf16le", 62, bin),
    CollationSeed {
        name: "binary",
        charset: "binary",
        id: BINARY_COLLATION_ID,
        binary: true,
        case_sensitive: true,
        accent_sensitive: true,
        sort_len: 1,
        pad_attribute: "NO PAD",
    },
    coll!("armscii8_bin", "armscii8", 64, bin),
    coll!("ascii_bin", "ascii", 65, bin),
    coll!("cp1250_bin", "cp1250", 66, bin),
    coll!("cp1256_bin", "cp1256", 67, bin),
    coll!("cp866_bin", "cp866", 68, bin),
    coll!("dec8_bin", "dec8", 69, bin),
    coll!("greek_bin", "greek", 70, bin),
    coll!("hebrew_bin", "hebrew", 71, bin),
    coll!("hp8_bin", "hp8", 72, bin),
    coll!("keybcs2_bin", "keybcs2", 73, bin),
    coll!("koi8r_bin", "koi8r", 74, bin),
    coll!("koi8u_bin", "koi8u", 75, bin),
    coll!("utf8mb3_tolower_ci", "utf8mb3", 76),
    coll!("latin2_bin", "latin2", 77, bin),
    coll!("latin5_bin", "latin5", 78, bin),
    coll!("latin7_bin", "latin7", 79, bin),
    coll!("cp850_bin", "cp850", 80, bin),
    coll!("cp852_bin", "cp852", 81, bin),
    coll!("swe7_bin", "swe7", 82, bin),
    coll!("utf8mb3_bin", "utf8mb3", 83, bin),
    coll!("big5_bin", "big5", 84, bin),
    coll!("euckr_bin", "euckr", 85, bin),
    coll!("gb2312_bin", "gb2312", 86, bin),
    coll!("gbk_bin", "gbk", 87, bin),
    coll!("sjis_bin", "sjis", 88, bin),
    coll!("tis620_bin", "tis620", 89, bin),
    coll!("ucs2_bin", "ucs2", 90, bin),
    coll!("ujis_bin", "ujis", 91, bin),
    coll!("geostd8_general_ci", "geostd8", 92),
    coll!("geostd8_bin", "geostd8", 93, bin),
    coll!("latin1_spanish_ci", "latin1", 94),
    coll!("cp932_japanese_ci", "cp932", 95),
    coll!("cp932_bin", "cp932", 96, bin),
    coll!("eucjpms_japanese_ci", "eucjpms", 97),
    coll!("eucjpms_bin", "eucjpms", 98, bin),
    coll!("cp1250_polish_ci", "cp1250", 99),
    coll!("ucs2_general_mysql500_ci", "ucs2", 159),
    coll!("utf8mb3_general_mysql500_ci", "utf8mb3", 223),
    coll!("gb18030_chinese_ci", "gb18030", 248),
    coll!("gb18030_bin", "gb18030", 249, bin),
    coll!("gb18030_unicode_520_ci", "gb18030", 250),
];

static UCA_SUFFIXES: &[&str] = &[
    "unicode_ci",
    "icelandic_ci",
    "latvian_ci",
    "romanian_ci",
    "slovenian_ci",
    "polish_ci",
    "estonian_ci",
    "spanish_ci",
    "swedish_ci",
    "turkish_ci",
    "czech_ci",
    "danish_ci",
    "lithuanian_ci",
    "slovak_ci",
    "spanish2_ci",
    "roman_ci",
    "persian_ci",
    "esperanto_ci",
    "hungarian_ci",
    "sinhala_ci",
    "german2_ci",
    "croatian_ci",
    "unicode_520_ci",
    "vietnamese_ci",
];

static UTF8MB4_0900_SUFFIXES: &[(u16, &str)] = &[
    (255, "0900_ai_ci"),
    (256, "de_pb_0900_ai_ci"),
    (257, "is_0900_ai_ci"),
    (258, "lv_0900_ai_ci"),
    (259, "ro_0900_ai_ci"),
    (260, "sl_0900_ai_ci"),
    (261, "pl_0900_ai_ci"),
    (262, "et_0900_ai_ci"),
    (263, "es_0900_ai_ci"),
    (264, "sv_0900_ai_ci"),
    (265, "tr_0900_ai_ci"),
    (266, "cs_0900_ai_ci"),
    (267, "da_0900_ai_ci"),
    (268, "lt_0900_ai_ci"),
    (269, "sk_0900_ai_ci"),
    (270, "es_trad_0900_ai_ci"),
    (271, "la_0900_ai_ci"),
    (273, "eo_0900_ai_ci"),
    (274, "hu_0900_ai_ci"),
    (275, "hr_0900_ai_ci"),
    (277, "vi_0900_ai_ci"),
    (278, "0900_as_cs"),
    (279, "de_pb_0900_as_cs"),
    (280, "is_0900_as_cs"),
    (281, "lv_0900_as_cs"),
    (282, "ro_0900_as_cs"),
    (283, "sl_0900_as_cs"),
    (284, "pl_0900_as_cs"),
    (285, "et_0900_as_cs"),
    (286, "es_0900_as_cs"),
    (287, "sv_0900_as_cs"),
    (288, "tr_0900_as_cs"),
    (289, "cs_0900_as_cs"),
    (290, "da_0900_as_cs"),
    (291, "lt_0900_as_cs"),
    (292, "sk_0900_as_cs"),
    (293, "es_trad_0900_as_cs"),
    (294, "la_0900_as_cs"),
    (296, "eo_0900_as_cs"),
    (297, "hu_0900_as_cs"),
    (298, "hr_0900_as_cs"),
    (300, "vi_0900_as_cs"),
    (303, "ja_0900_as_cs"),
    (304, "ja_0900_as_cs_ks"),
    (305, "0900_as_ci"),
    (306, "ru_0900_ai_ci"),
    (307, "ru_0900_as_cs"),
    (308, "zh_0900_as_cs"),
    (309, "0900_bin"),
    (310, "nb_0900_ai_ci"),
    (311, "nb_0900_as_cs"),
    (312, "nn_0900_ai_ci"),
    (313, "nn_0900_as_cs"),
    (314, "sr_latn_0900_ai_ci"),
    (315, "sr_latn_0900_as_cs"),
    (316, "bs_0900_ai_ci"),
    (317, "bs_0900_as_cs"),
    (318, "bg_0900_ai_ci"),
    (319, "bg_0900_as_cs"),
    (320, "gl_0900_ai_ci"),
    (321, "gl_0900_as_cs"),
    (322, "mn_cyrl_0900_ai_ci"),
    (323, "mn_cyrl_0900_as_cs"),
];

static COLLATIONS: OnceLock<Vec<CollationInfo>> = OnceLock::new();
type NameIndex = HashMap<&'static str, usize, ahash::RandomState>;

static CHARSET_BY_NAME: OnceLock<NameIndex> = OnceLock::new();
static COLLATION_BY_NAME: OnceLock<NameIndex> = OnceLock::new();
static COLLATION_BY_ID: OnceLock<HashMap<u16, usize>> = OnceLock::new();

/// Highest known compiled collation id (`mn_cyrl_0900_as_cs = 323`) plus a
/// small headroom for any future MySQL allocations. Resolving an id within
/// this range is O(1) array lookup instead of HashMap probing.
const COLLATION_ID_DIRECT_LIMIT: usize = 384;

/// Dense `id -> &CollationInfo` table for ids in [0, COLLATION_ID_DIRECT_LIMIT).
/// Built once on first use. Every id MySQL 8.0 ships today fits here, so the
/// HashMap fallback only fires for unknown / out-of-range ids.
static COLLATION_BY_ID_DIRECT: OnceLock<Box<[Option<&'static CollationInfo>]>> = OnceLock::new();

/// `&'static CollationInfo` for `DEFAULT_COLLATION`. Hot paths
/// (`default_string_key`, `default_compare_strings`, `Value::String` ↔
/// `ValueKey` conversion) hit this on every row.
static DEFAULT_COLLATION_INFO: OnceLock<&'static CollationInfo> = OnceLock::new();

thread_local! {
    /// Reusable scratch buffer for `with_collation_key_by_id` /
    /// `with_default_string_key`. Lets every hot path produce a collation
    /// key once per row without allocating: hash, B-tree memcmp encoding,
    /// `ValueKey` construction, statistics digesting all share one
    /// thread-local `Vec<u8>` per thread.
    static COLLATION_KEY_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn collations() -> &'static [CollationInfo] {
    COLLATIONS.get_or_init(build_collations).as_slice()
}

fn build_collations() -> Vec<CollationInfo> {
    let mut out = Vec::with_capacity(320);
    for seed in BASE_COLLATIONS {
        out.push(seed_to_collation(*seed));
    }
    add_uca_family(&mut out, "utf16", 101);
    add_uca_family(&mut out, "ucs2", 128);
    add_uca_family(&mut out, "utf32", 160);
    add_uca_family(&mut out, "utf8mb3", 192);
    add_uca_family(&mut out, "utf8mb4", 224);
    for (id, suffix) in UTF8MB4_0900_SUFFIXES {
        let name = leak_string(format!("utf8mb4_{suffix}"));
        let binary = name.ends_with("_bin");
        let case_sensitive = binary || name.contains("_as_cs");
        let accent_sensitive = binary || name.contains("_as_");
        let german_phonebook = name.contains("german2") || name.contains("de_pb_");
        out.push(CollationInfo {
            name,
            charset: "utf8mb4",
            id: *id,
            is_default: name == DEFAULT_COLLATION,
            is_compiled: true,
            sort_len: 0,
            pad_attribute: "NO PAD",
            case_sensitive,
            accent_sensitive,
            binary,
            uca_0900: true,
            german_phonebook,
            common_equivalences: !accent_sensitive,
            pad_space: false,
        });
    }
    out.sort_by_key(|collation| collation.id);
    out
}

fn add_uca_family(out: &mut Vec<CollationInfo>, charset: &'static str, first_id: u16) {
    for (offset, suffix) in UCA_SUFFIXES.iter().enumerate() {
        let id = first_id + offset as u16;
        let name = leak_string(format!("{charset}_{suffix}"));
        let german_phonebook = name.contains("german2") || name.contains("de_pb_");
        out.push(CollationInfo {
            name,
            charset,
            id,
            is_default: false,
            is_compiled: true,
            sort_len: 8,
            pad_attribute: "PAD SPACE",
            case_sensitive: false,
            accent_sensitive: false,
            binary: false,
            uca_0900: false,
            german_phonebook,
            common_equivalences: name.contains("_unicode_") || german_phonebook,
            pad_space: true,
        });
    }
}

fn seed_to_collation(seed: CollationSeed) -> CollationInfo {
    let german_phonebook = seed.name.contains("german2") || seed.name.contains("de_pb_");
    let uca_0900 = seed.name.contains("_0900_");
    CollationInfo {
        name: seed.name,
        charset: seed.charset,
        id: seed.id,
        is_default: default_collation_name(seed.charset) == Some(seed.name),
        is_compiled: true,
        sort_len: seed.sort_len,
        pad_attribute: seed.pad_attribute,
        case_sensitive: seed.case_sensitive,
        accent_sensitive: seed.accent_sensitive,
        binary: seed.binary,
        uca_0900,
        german_phonebook,
        common_equivalences: !seed.accent_sensitive
            && (uca_0900 || seed.name.contains("_unicode_") || german_phonebook),
        pad_space: compute_pad_space(seed.pad_attribute),
    }
}

fn leak_string(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

fn name_index<T>(entries: &[T], name: impl Fn(&T) -> &'static str) -> NameIndex {
    let mut index = NameIndex::with_capacity_and_hasher(entries.len() * 2, Default::default());
    index.extend(
        entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| (name(entry), idx)),
    );
    index
}

fn charset_map() -> &'static NameIndex {
    CHARSET_BY_NAME.get_or_init(|| name_index(CHARSETS, |charset| charset.name))
}

fn collation_name_map() -> &'static NameIndex {
    COLLATION_BY_NAME.get_or_init(|| name_index(collations(), |collation| collation.name))
}

fn collation_id_map() -> &'static HashMap<u16, usize> {
    COLLATION_BY_ID.get_or_init(|| {
        let mut map = HashMap::with_capacity(collations().len());
        for (idx, collation) in collations().iter().enumerate() {
            map.insert(collation.id, idx);
        }
        map
    })
}

fn collation_id_direct() -> &'static [Option<&'static CollationInfo>] {
    COLLATION_BY_ID_DIRECT.get_or_init(|| {
        let mut table: Vec<Option<&'static CollationInfo>> = vec![None; COLLATION_ID_DIRECT_LIMIT];
        for collation in collations() {
            let idx = collation.id as usize;
            if idx < COLLATION_ID_DIRECT_LIMIT {
                table[idx] = Some(collation);
            }
        }
        table.into_boxed_slice()
    })
}

/// `&'static CollationInfo` for the default collation. Cached at the
/// first call so hot paths (`default_string_key`, `default_compare_strings`,
/// `ValueKey::from(&Value::String)`) can skip the `resolve_collation` /
/// HashMap lookup that every row would otherwise pay.
#[inline]
pub fn default_collation_info() -> &'static CollationInfo {
    DEFAULT_COLLATION_INFO
        .get_or_init(|| resolve_collation(DEFAULT_COLLATION).expect("default collation exists"))
}

/// True when the default collation compares case-insensitively. Drives
/// the collation-aware paths of LIKE and the string search functions
/// (`LOCATE` / `FIND_IN_SET` / …), which must agree with what `=`
/// comparisons (`default_compare_strings`) already do.
#[inline]
pub fn default_collation_is_ci() -> bool {
    let info = default_collation_info();
    !info.binary && !info.case_sensitive
}

/// Per-character equality under the default collation, for LIKE and
/// the substring-search functions. MySQL's LIKE matches character by
/// character — no multi-char weight expansions, so `'ß' LIKE 'ss'` is
/// false even though `'ß' = 'ss'` is true — which makes single-char
/// collation-key equality the exact semantic. ASCII pairs
/// short-circuit without touching the key writer.
pub fn like_chars_equal(a: char, b: char) -> bool {
    like_chars_equal_for_info(default_collation_info(), a, b)
}

/// Same as [`like_chars_equal`] but honours an explicit `CollationInfo`
/// rather than the process-wide default. Drives the collation-aware
/// LIKE matcher — see `engine::expr::match_like_pattern_with_info`.
///
/// Binary collations compare bytes verbatim (no case/accent folding).
/// ASCII pairs short-circuit through `eq_ignore_ascii_case` when the
/// collation is CI; non-ASCII pairs fall through to the collation's
/// `compare_strings_for_info` for MySQL-compatible weight equality.
#[inline]
pub fn like_chars_equal_for_info(info: &CollationInfo, a: char, b: char) -> bool {
    if a == b {
        return true;
    }
    if info.binary {
        return false;
    }
    if info.id == UTF8MB4_0900_AI_CI_ID {
        let mut a_buf = [0; 4];
        let mut b_buf = [0; 4];
        return uca9::weights(a.encode_utf8(&mut a_buf))
            .eq(uca9::weights(b.encode_utf8(&mut b_buf)));
    }
    if a.is_ascii() && b.is_ascii() {
        return !info.case_sensitive && a.eq_ignore_ascii_case(&b);
    }
    let mut abuf = [0u8; 4];
    let mut bbuf = [0u8; 4];
    compare_strings_for_info(info, a.encode_utf8(&mut abuf), b.encode_utf8(&mut bbuf))
        == Ordering::Equal
}

/// Equality between one Unicode input character and one ASCII pattern
/// character under `info`'s LIKE semantics, without materialising either
/// collation key.
///
/// Constant ASCII LIKE patterns are common even when the scanned text is not
/// ASCII. The general comparator builds two `Vec<u8>` keys for every
/// non-ASCII candidate character; a leading `%` can make that happen for
/// every character in every row. This streams the exact single-character key
/// transformation and succeeds only when it emits the pattern's one-byte
/// key. Multi-character expansions (for example `ß -> ss`) therefore remain
/// non-equal, as required by MySQL's character-at-a-time LIKE semantics.
pub(crate) fn like_char_equals_ascii_for_info(
    info: &CollationInfo,
    value: char,
    pattern: u8,
) -> bool {
    debug_assert!(pattern.is_ascii());
    if info.id == UTF8MB4_0900_AI_CI_ID {
        let mut buf = [0; 4];
        let pattern = [pattern];
        return uca9::weights(value.encode_utf8(&mut buf))
            .eq(uca9::weights(std::str::from_utf8(&pattern).unwrap()));
    }
    if value.is_ascii() {
        let value = value as u8;
        return if info.binary || info.case_sensitive {
            value == pattern
        } else {
            value.eq_ignore_ascii_case(&pattern)
        };
    }
    if info.binary {
        return false;
    }
    // PAD SPACE and UCA-9.0 NUL can turn a one-byte pattern into an empty
    // comparison key. They are rare in LIKE literals and are subtle enough
    // to keep on the existing oracle path.
    if (info.pad_space && pattern == b' ') || (info.uca_0900 && pattern == b'\0') {
        return like_chars_equal_for_info(info, value, pattern as char);
    }
    let expected = if info.case_sensitive {
        pattern
    } else {
        pattern.to_ascii_lowercase()
    };
    like_char_ascii_weight_for_info(info, value) == Some(expected)
}

/// Return the complete single-character collation key when it is exactly one
/// ASCII byte. `None` represents an empty, multi-byte, or non-ASCII key.
/// Callers matching a compiled ASCII LIKE pattern may memoize this result by
/// `(collation, value)` and compare it with different pattern bytes.
pub(crate) fn like_char_ascii_weight_for_info(info: &CollationInfo, value: char) -> Option<u8> {
    if info.id == UTF8MB4_0900_AI_CI_ID {
        let mut buf = [0; 4];
        let mut weights = uca9::weights(value.encode_utf8(&mut buf));
        let first = weights.next()?;
        if weights.next().is_some() {
            return None;
        }
        return (0u8..128)
            .find(|byte| {
                let pattern = [*byte];
                let mut pattern_weights = uca9::weights(std::str::from_utf8(&pattern).unwrap());
                pattern_weights.next() == Some(first) && pattern_weights.next().is_none()
            })
            .map(|byte| byte.to_ascii_lowercase());
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        let primary = legacy_general::primary(value);
        return (primary < 128).then(|| (primary as u8).to_ascii_lowercase());
    }
    if value.is_ascii() {
        let byte = value as u8;
        return Some(if info.binary || info.case_sensitive {
            byte
        } else {
            byte.to_ascii_lowercase()
        });
    }
    if info.binary {
        return None;
    }
    let mut emitted = 0usize;
    let mut weight = 0u8;
    let mut valid = true;
    let mut emit = |bytes: &[u8]| {
        for &byte in bytes {
            if emitted == 0 && byte.is_ascii() {
                weight = byte;
            } else {
                valid = false;
            }
            emitted += 1;
        }
    };
    let (tailored, tailored_len) = if info.german_phonebook {
        german_phonebook_expansion(value)
    } else {
        ([value, '\0'], 1)
    };
    for tailored in tailored.into_iter().take(tailored_len) {
        if info.accent_sensitive {
            emit_collation_case_char(info, tailored, false, &mut emit);
        } else {
            unicode_normalization::char::decompose_compatible(tailored, |normalized| {
                if !is_combining_mark(normalized) {
                    emit_collation_case_char(info, normalized, info.common_equivalences, &mut emit);
                }
            });
        }
    }
    (valid && emitted == 1).then_some(weight)
}

pub fn normalize_charset_name(name: &str) -> String {
    match name
        .trim_matches('\'')
        .trim_matches('"')
        .to_ascii_lowercase()
        .as_str()
    {
        "utf8" => "utf8mb3".to_string(),
        "utf-8" => "utf8mb3".to_string(),
        other => other.to_string(),
    }
}

pub fn normalize_collation_name(name: &str) -> String {
    let normalized = name
        .trim_matches('\'')
        .trim_matches('"')
        .to_ascii_lowercase();
    if let Some(rest) = normalized.strip_prefix("utf8_") {
        format!("utf8mb3_{rest}")
    } else {
        normalized
    }
}

pub fn resolve_charset(name: &str) -> Result<&'static CharsetInfo, QueryError> {
    // `normalize_charset_name` allocates twice (lowercase fold + owned
    // result). This runs on the session's `character_set_client` for every
    // decoded COM_QUERY and again per result set for the fallback collation,
    // where the name is already in canonical form and the map lookup can
    // borrow it directly. Try that first and only fall back to the
    // normalizing path for the quoted / mixed-case / `utf8` alias spellings.
    if let Some(idx) = charset_map().get(name) {
        return Ok(&CHARSETS[*idx]);
    }
    let normalized = normalize_charset_name(name);
    charset_map()
        .get(normalized.as_str())
        .map(|idx| &CHARSETS[*idx])
        .ok_or_else(|| unknown_charset(name))
}

pub fn resolve_client_charset(name: &str) -> Result<&'static CharsetInfo, QueryError> {
    let charset = resolve_charset(name)?;
    if !charset.client_allowed {
        return Err(mysql_errors::error(
            mysql_errors::ER_WRONG_VALUE_FOR_VAR,
            format!(
                "Variable 'character_set_client' can't be set to the value of '{}'",
                charset.name
            ),
        ));
    }
    Ok(charset)
}

pub fn resolve_collation(name: &str) -> Result<&'static CollationInfo, QueryError> {
    let normalized = normalize_collation_name(name);
    collation_name_map()
        .get(normalized.as_str())
        .map(|idx| &collations()[*idx])
        .ok_or_else(|| unknown_collation(name))
}

pub fn default_collation_for_charset(charset: &str) -> Result<&'static str, QueryError> {
    Ok(resolve_charset(charset)?.default_collation)
}

fn default_collation_name(charset: &str) -> Option<&'static str> {
    CHARSETS
        .iter()
        .find(|info| info.name == charset)
        .map(|info| info.default_collation)
}

pub fn validate_charset_collation_pair(
    charset: &str,
    collation: &str,
) -> Result<(&'static CharsetInfo, &'static CollationInfo), QueryError> {
    let charset_info = resolve_charset(charset)?;
    let collation_info = resolve_collation(collation)?;
    if !collation_info
        .charset
        .eq_ignore_ascii_case(charset_info.name)
    {
        return Err(mysql_errors::error(
            mysql_errors::ER_COLLATION_CHARSET_MISMATCH,
            format!(
                "COLLATION '{}' is not valid for CHARACTER SET '{}'",
                collation, charset
            ),
        ));
    }
    Ok((charset_info, collation_info))
}

pub fn collation_id(name: &str) -> u16 {
    resolve_collation(name)
        .map(|collation| collation.id)
        .unwrap_or(UTF8MB4_0900_AI_CI_ID)
}

pub fn resolve_collation_id(id: u16) -> Option<&'static CollationInfo> {
    // Dense-array fast path covers every MySQL 8.0 compiled collation id.
    // The HashMap fallback only ever fires for ids outside that range,
    // which today means none — but it's kept so future MySQL upgrades
    // that add ids above the direct-lookup limit still resolve correctly.
    let id_idx = id as usize;
    if id_idx < COLLATION_ID_DIRECT_LIMIT {
        if let Some(info) = collation_id_direct()[id_idx] {
            return Some(info);
        }
        return None;
    }
    collation_id_map().get(&id).map(|idx| &collations()[*idx])
}

pub fn resolve_collation_id_checked(id: u16) -> Result<&'static CollationInfo, QueryError> {
    resolve_collation_id(id).ok_or_else(|| {
        mysql_errors::error(
            mysql_errors::ER_UNKNOWN_COLLATION,
            format!("unknown collation id: {id}"),
        )
    })
}

pub fn collation_name_for_id(id: u16) -> &'static str {
    resolve_collation_id(id)
        .map(|collation| collation.name)
        .unwrap_or(DEFAULT_COLLATION)
}

pub fn character_sets_rows() -> Vec<Vec<Value>> {
    CHARSETS
        .iter()
        .map(|charset| {
            vec![
                Value::String(charset.name.to_string()),
                Value::String(charset.default_collation.to_string()),
                Value::String(charset.description.to_string()),
                Value::Int(charset.max_len as i64),
            ]
        })
        .collect()
}

pub fn collations_rows() -> Vec<Vec<Value>> {
    collations()
        .iter()
        .map(|collation| {
            vec![
                Value::String(collation.name.to_string()),
                Value::String(collation.charset.to_string()),
                Value::Int(collation.id as i64),
                Value::String(if collation.is_default { "Yes" } else { "" }.to_string()),
                Value::String(if collation.is_compiled { "Yes" } else { "" }.to_string()),
                Value::Int(collation.sort_len as i64),
                Value::String(collation.pad_attribute.to_string()),
            ]
        })
        .collect()
}

pub fn collation_applicability_rows() -> Vec<Vec<Value>> {
    collations()
        .iter()
        .map(|collation| {
            vec![
                Value::String(collation.name.to_string()),
                Value::String(collation.charset.to_string()),
            ]
        })
        .collect()
}

pub fn validate_text_for_charset(charset: &str, value: &str) -> Result<(), QueryError> {
    // Fast path for natively-UTF-8 / binary charsets: `value: &str` is
    // already valid UTF-8, so utf8mb4 and binary accept it unconditionally.
    // Skips the `normalize_charset_name` String alloc + HashMap lookup +
    // throwaway `Vec<u8>` allocation that `encode_string` would do.
    // Profile (TPC-H SF=1 LOAD DATA lineitem, 2026-06-03): `encode_string`
    // accounted for ~15% of serial LOAD CPU; the default charset is
    // `utf8mb4` so this fast path absorbs the bulk of it.
    if charset.len() == 7 {
        let bytes = charset.as_bytes();
        if bytes.eq_ignore_ascii_case(b"utf8mb4") {
            return Ok(());
        }
    }
    if charset.len() == 6 && charset.as_bytes().eq_ignore_ascii_case(b"binary") {
        return Ok(());
    }
    validate_text_encoding(charset, value)
}

// Keep charset resolution and encoder stack frames off the UTF-8/binary
// validation fast path. Other charsets still use the shared encoding rules.
#[inline(never)]
fn validate_text_encoding(charset: &str, value: &str) -> Result<(), QueryError> {
    let charset = resolve_charset(charset)?;
    match charset.name {
        // A Rust str already contains valid Unicode scalars. These encodings
        // accept all of them; BMP-only encodings only need the range check.
        "utf16" | "utf16le" | "utf32" => Ok(()),
        "utf8mb3" | "ucs2" => validate_unicode_range(charset.name, value),
        _ => encode_string_borrowed(charset, value)
            .map(|_| ())
            .ok_or_else(|| incorrect_string_value(charset.name)),
    }
}

pub fn decode_bytes(charset: &str, bytes: &[u8]) -> Result<String, QueryError> {
    let charset = resolve_charset(charset)?;
    match charset.name {
        "binary" => Ok(bytes.iter().map(|b| char::from(*b)).collect()),
        "utf8mb3" => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| incorrect_string_value(charset.name))?
                .to_string();
            validate_unicode_range(charset.name, &text)?;
            Ok(text)
        }
        "utf8mb4" => std::str::from_utf8(bytes)
            .map(|text| text.to_string())
            .map_err(|_| incorrect_string_value(charset.name)),
        "ucs2" => decode_ucs2_be(bytes).ok_or_else(|| incorrect_string_value(charset.name)),
        "utf16" => decode_utf16(bytes, false).ok_or_else(|| incorrect_string_value(charset.name)),
        "utf16le" => decode_utf16(bytes, true).ok_or_else(|| incorrect_string_value(charset.name)),
        "utf32" => decode_utf32_be(bytes).ok_or_else(|| incorrect_string_value(charset.name)),
        other => {
            let Some(encoding) = encoding_for_charset(other) else {
                return Ok(bytes.iter().map(|b| char::from(*b)).collect());
            };
            let (decoded, had_errors) = encoding.decode_without_bom_handling(bytes);
            if had_errors {
                return Err(incorrect_string_value(charset.name));
            }
            Ok(decoded.into_owned())
        }
    }
}

/// Validate source octets without materializing decoded text on the common
/// UTF-8/binary paths.  This is used at runtime conversion boundaries where
/// callers need an error but do not consume the decoded string.
pub fn validate_bytes_for_charset(charset: &str, bytes: &[u8]) -> Result<(), QueryError> {
    if charset.eq_ignore_ascii_case("binary") {
        return Ok(());
    }
    if charset.eq_ignore_ascii_case("utf8mb4") {
        return std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| incorrect_string_value("utf8mb4"));
    }
    if charset.eq_ignore_ascii_case("utf8mb3") || charset.eq_ignore_ascii_case("utf8") {
        let text = std::str::from_utf8(bytes).map_err(|_| incorrect_string_value("utf8mb3"))?;
        return validate_unicode_range("utf8mb3", text);
    }
    decode_bytes(charset, bytes).map(|_| ())
}

pub fn encode_string(charset: &str, value: &str) -> Result<Vec<u8>, QueryError> {
    let charset = resolve_charset(charset)?;
    encode_string_owned(charset, value, || incorrect_string_value(charset.name))
}

/// Encode an explicit SQL character-set conversion, replacing each source
/// character that the target cannot represent with a single `?` byte (or the
/// target encoding of `?`). Result-protocol conversion uses the same
/// replacement policy so metadata always agrees with the emitted bytes.
pub fn encode_string_with_replacement(charset: &str, value: &str) -> Result<Vec<u8>, QueryError> {
    let charset = resolve_charset(charset)?;
    if let Ok(encoded) = encode_string_owned(charset, value, || ()) {
        return Ok(encoded);
    }
    let replacement =
        encode_string_borrowed(charset, "?").ok_or_else(|| incorrect_string_value(charset.name))?;
    let mut out = Vec::with_capacity(value.len());
    for ch in value.chars() {
        let mut buf = [0u8; 4];
        let text = ch.encode_utf8(&mut buf);
        match encode_string_borrowed(charset, text) {
            Some(encoded) => out.extend_from_slice(&encoded),
            None => out.extend_from_slice(&replacement),
        }
    }
    Ok(out)
}

/// Resolve a result/connection charset name once into its static
/// descriptor so the per-cell encode path (`encode_string_lossy_cow_info`)
/// can skip the allocating `normalize_charset_name` + hash lookup that
/// `resolve_charset` performs. An unknown name resolves to `None` — the
/// same "borrow the source bytes verbatim" fallback the name-based path
/// took on a failed resolve, so behaviour is unchanged. Callers on the
/// result-set hot path resolve this once per result set and thread the
/// descriptor down to every row/cell.
#[inline]
pub fn resolve_result_charset(name: Option<&str>) -> Option<&'static CharsetInfo> {
    name.and_then(|n| resolve_charset(n).ok())
}

/// Like `encode_string_lossy_cow` but takes an already-resolved charset
/// descriptor, avoiding the per-call `resolve_charset`. This is the hot
/// path used by the result-set row encoder.
#[inline]
pub fn encode_string_lossy_cow_info<'a>(
    charset: Option<&'static CharsetInfo>,
    value: &'a str,
) -> Cow<'a, [u8]> {
    match charset {
        Some(info) => encode_string_borrowed(info, value).unwrap_or_else(|| {
            Cow::Owned(
                encode_string_with_replacement(info.name, value)
                    .expect("resolved result charset supports replacement"),
            )
        }),
        None => Cow::Borrowed(value.as_bytes()),
    }
}

/// Encode `value` under an already-resolved `charset`, returning
/// `Some(Cow::Borrowed)` when the input bytes already satisfy the charset
/// (every supported natively-UTF-8 charset on the fast path),
/// `Some(Cow::Owned)` when an allocation is genuinely required, or `None`
/// when the encoding would fail. Lossy callers can then replace characters
/// outside the target repertoire without mislabeling UTF-8 bytes.
#[inline]
fn encode_string_borrowed<'a>(
    charset: &'static CharsetInfo,
    value: &'a str,
) -> Option<Cow<'a, [u8]>> {
    encode_string_as(charset, value, Cow::Borrowed, Cow::Owned, || ()).ok()
}

#[inline(always)]
fn encode_string_owned<E>(
    charset: &'static CharsetInfo,
    value: &str,
    invalid: impl FnOnce() -> E,
) -> Result<Vec<u8>, E> {
    encode_string_as(
        charset,
        value,
        |bytes| bytes.to_vec(),
        |bytes| bytes,
        invalid,
    )
}

// Choose ownership and failure handling at compile time: strict callers return
// Vec and QueryError directly; probing callers can borrow and fail without allocation.
#[inline(always)]
fn encode_string_as<'a, T, E>(
    charset: &'static CharsetInfo,
    value: &'a str,
    borrowed: impl FnOnce(&'a [u8]) -> T,
    owned: impl FnOnce(Vec<u8>) -> T,
    invalid: impl FnOnce() -> E,
) -> Result<T, E> {
    match charset.name {
        // Natively-UTF-8 storage layouts — the protocol payload is the
        // source bytes verbatim, so we can borrow.
        "binary" | "utf8mb4" => Ok(borrowed(value.as_bytes())),
        "utf16" => Ok(owned(encode_utf16(value, false))),
        "utf16le" => Ok(owned(encode_utf16(value, true))),
        "utf32" => Ok(owned(encode_utf32_be(value))),
        "ucs2" => encode_ucs2_be(value).map(owned).ok_or_else(invalid),
        "utf8mb3" => is_bmp_text(value)
            .then(|| borrowed(value.as_bytes()))
            .ok_or_else(invalid),
        "ascii" | "swe7" => {
            if value.is_ascii() {
                Ok(borrowed(value.as_bytes()))
            } else {
                Err(invalid())
            }
        }
        other => {
            if let Some(encoding) = encoding_for_charset(other) {
                let (encoded, _encoding, had_errors) = encoding.encode(value);
                if had_errors {
                    return Err(invalid());
                }
                // `encoding_rs::encode` returns `Cow<[u8]>` already —
                // when no transcoding was needed it's borrowed, and the
                // borrow is *into the input string*, so we can lift it
                // directly into our `'a` lifetime.
                Ok(match encoded {
                    Cow::Borrowed(bytes) => borrowed(bytes),
                    Cow::Owned(bytes) => owned(bytes),
                })
            } else if value.chars().all(|ch| (ch as u32) <= 0xff) {
                Ok(owned(value.chars().map(|ch| ch as u8).collect()))
            } else {
                Err(invalid())
            }
        }
    }
}

fn decode_utf16(bytes: &[u8], little_endian: bool) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    let units = bytes.chunks_exact(2).map(|chunk| {
        let pair = [chunk[0], chunk[1]];
        if little_endian {
            u16::from_le_bytes(pair)
        } else {
            u16::from_be_bytes(pair)
        }
    });
    std::char::decode_utf16(units)
        .collect::<Result<String, _>>()
        .ok()
}

fn decode_ucs2_be(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 2 != 0 {
        return None;
    }
    bytes
        .chunks_exact(2)
        .map(|chunk| char::from_u32(u16::from_be_bytes([chunk[0], chunk[1]]) as u32))
        .collect()
}

fn decode_utf32_be(bytes: &[u8]) -> Option<String> {
    if bytes.len() % 4 != 0 {
        return None;
    }
    bytes
        .chunks_exact(4)
        .map(|chunk| char::from_u32(u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])))
        .collect()
}

fn encode_utf16(value: &str, little_endian: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len().saturating_mul(2));
    for unit in value.encode_utf16() {
        let bytes = if little_endian {
            unit.to_le_bytes()
        } else {
            unit.to_be_bytes()
        };
        out.extend_from_slice(&bytes);
    }
    out
}

fn encode_ucs2_be(value: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(value.len().saturating_mul(2));
    for ch in value.chars() {
        let unit = u16::try_from(ch as u32).ok()?;
        if (0xD800..=0xDFFF).contains(&unit) {
            return None;
        }
        out.extend_from_slice(&unit.to_be_bytes());
    }
    Some(out)
}

fn encode_utf32_be(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.chars().count().saturating_mul(4));
    for ch in value.chars() {
        out.extend_from_slice(&(ch as u32).to_be_bytes());
    }
    out
}

fn is_bmp_text(value: &str) -> bool {
    value.chars().all(|ch| (ch as u32) <= 0xffff)
}

fn validate_unicode_range(charset: &str, value: &str) -> Result<(), QueryError> {
    match charset {
        "utf8mb3" | "ucs2" if !is_bmp_text(value) => Err(incorrect_string_value(charset)),
        _ => Ok(()),
    }
}

fn encoding_for_charset(charset: &str) -> Option<&'static Encoding> {
    let label = match charset {
        "big5" => "big5",
        "cp1250" => "windows-1250",
        "cp1251" => "windows-1251",
        "cp1256" => "windows-1256",
        "cp1257" => "windows-1257",
        "cp932" | "sjis" => "shift_jis",
        "euckr" => "euc-kr",
        "eucjpms" | "ujis" => "euc-jp",
        "gb18030" => "gb18030",
        "gb2312" | "gbk" => "gbk",
        "greek" => "iso-8859-7",
        "hebrew" => "iso-8859-8",
        "koi8r" => "koi8-r",
        "koi8u" => "koi8-u",
        "latin1" => "windows-1252",
        "latin2" => "iso-8859-2",
        "latin5" => "windows-1254",
        "latin7" => "iso-8859-13",
        "tis620" => "windows-874",
        _ => return None,
    };
    Encoding::for_label(label.as_bytes())
}

pub fn compare_strings(collation: &str, left: &str, right: &str) -> Ordering {
    let info = resolve_collation(collation)
        .ok()
        .unwrap_or_else(|| default_collation_info());
    compare_strings_for_info(info, left, right)
}

pub fn compare_strings_by_id(collation_id: u16, left: &str, right: &str) -> Ordering {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    compare_strings_for_info(info, left, right)
}

/// Compare two already-decoded strings with a pre-resolved collation.
///
/// Most callers should use [`compare_strings_by_id`]. Scan predicates that
/// execute once per row resolve the id when the predicate is compiled and use
/// this entry directly, avoiding a redundant dense-table lookup in the hot
/// loop.
pub(crate) fn compare_strings_for_info(info: &CollationInfo, left: &str, right: &str) -> Ordering {
    let left = trim_for_pad(info, left);
    let right = trim_for_pad(info, right);
    if info.binary {
        return left.as_bytes().cmp(right.as_bytes());
    }
    if info.id == UTF8MB4_0900_AI_CI_ID {
        return uca9::compare(left, right);
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        return left
            .chars()
            .map(legacy_general::primary)
            .cmp(right.chars().map(legacy_general::primary));
    }
    // ASCII fast path: while both sides are pure ASCII, NFD / Unicode
    // to_lowercase / expand_common_equivalences are all identity on the
    // bytes (no combining marks possible; ASCII upper↔lower is a one-byte
    // toggle; none of the special equivalence chars are ASCII). The
    // fused walk decides at the first differing byte without first
    // scanning both full strings for ASCII-ness — sorts compare each
    // key pair O(log n) times and almost always diverge early, so the
    // two up-front `is_ascii()` passes used to dominate this function.
    // UCA 9.0 assigns U+0000 an ignorable collation element.  The fused
    // comparator detects a zero byte while walking the already-needed common
    // prefix and only then switches to its skip-NUL lane.  The old up-front
    // `contains(0)` scan read both complete CHAR values before virtually every
    // sort comparison, even though random keys usually differ in their first
    // few bytes.
    if let Some(ord) = ascii_cmp_fused(
        left.as_bytes(),
        right.as_bytes(),
        info.case_sensitive,
        is_uca_0900_collation(info),
    ) {
        return ord;
    }
    compare_non_ascii_strings_for_info(info, left, right)
}

// Keep Unicode iterator state out of the ASCII comparator's stack frame.
// Inputs have already been trimmed according to the collation's PAD rule.
#[inline(never)]
fn compare_non_ascii_strings_for_info(info: &CollationInfo, left: &str, right: &str) -> Ordering {
    // These weights are concatenated per scalar: accent-insensitive keys
    // discard combining marks, and accent-sensitive keys preserve scalar
    // order. An identical scalar prefix therefore cancels before comparison.
    // This also avoids transforming long shared Unicode prefixes at all.
    let mut common = common_collation_input_prefix(left.as_bytes(), right.as_bytes());
    while !left.is_char_boundary(common) {
        common -= 1;
    }
    let (left, right) = (&left[common..], &right[common..]);
    if common > 0 {
        if let Some(ord) = ascii_cmp_fused(
            left.as_bytes(),
            right.as_bytes(),
            info.case_sensitive,
            is_uca_0900_collation(info),
        ) {
            return ord;
        }
    }
    compare_unicode_weights(info, left, right)
}

// Shared-prefix comparisons usually finish above without constructing either
// iterator. Keep their frame small as well as the all-ASCII caller's frame.
#[inline(never)]
fn compare_unicode_weights(info: &CollationInfo, left: &str, right: &str) -> Ordering {
    CollationWeights::new(info, left).cmp(CollationWeights::new(info, right))
}

fn common_collation_input_prefix(left: &[u8], right: &[u8]) -> usize {
    let len = left.len().min(right.len());
    let mut offset = 0;
    while len - offset >= 8 {
        let left = u64::from_le_bytes(left[offset..offset + 8].try_into().unwrap());
        let right = u64::from_le_bytes(right[offset..offset + 8].try_into().unwrap());
        let different = left ^ right;
        if different != 0 {
            return offset + (different.trailing_zeros() / 8) as usize;
        }
        offset += 8;
    }
    offset
        + left[offset..len]
            .iter()
            .zip(&right[offset..len])
            .take_while(|(left, right)| left == right)
            .count()
}

/// Lazily emit the same bytes as the Unicode key writer. Sort and B-tree
/// comparisons usually differ near the start, so materializing both complete
/// keys repeats normalization and allocation for every comparison. Keep only
/// one scalar's expansion and stop at the first differing weight byte.
struct CollationWeights<'a> {
    info: &'a CollationInfo,
    chars: std::str::Chars<'a>,
    pending: smallvec::SmallVec<[u8; 32]>,
    position: usize,
}

impl<'a> CollationWeights<'a> {
    fn new(info: &'a CollationInfo, value: &'a str) -> Self {
        Self {
            info,
            chars: value.chars(),
            pending: smallvec::SmallVec::new(),
            position: 0,
        }
    }

    fn refill(&mut self, ch: char) {
        let info = self.info;
        self.pending.clear();
        self.position = 0;
        let pending = &mut self.pending;
        if info.id == UTF8MB4_0900_AI_CI_ID {
            uca9::scalar(ch, &mut |weight| {
                let (bytes, len) = uca9::key_unit(weight);
                pending.extend_from_slice(&bytes[..len]);
            });
            return;
        }
        let mut emit = |bytes: &[u8]| pending.extend_from_slice(bytes);
        let (tailored, len) = if info.german_phonebook {
            german_phonebook_expansion(ch)
        } else {
            ([ch, '\0'], 1)
        };
        for ch in tailored.into_iter().take(len) {
            if info.accent_sensitive {
                emit_collation_case_char(info, ch, false, &mut emit);
            } else {
                // Canonical ordering only reorders combining marks. They
                // are discarded here, so per-scalar decomposition gives
                // exactly the full string's accent-insensitive key.
                decompose_compatible(ch, |normalized| {
                    if !is_combining_mark(normalized) {
                        emit_collation_case_char(
                            info,
                            normalized,
                            info.common_equivalences,
                            &mut emit,
                        );
                    }
                });
            }
        }
    }
}

impl Iterator for CollationWeights<'_> {
    type Item = u8;

    #[inline]
    fn next(&mut self) -> Option<u8> {
        loop {
            if let Some(&byte) = self.pending.get(self.position) {
                self.position += 1;
                return Some(byte);
            }
            let ch = self.chars.next()?;
            let info = self.info;
            if is_uca_0900_collation(info) && ch == '\0' {
                continue;
            }
            if info.id == UTF8MB4_0900_AI_CI_ID {
                self.refill(ch);
                continue;
            }
            // ASCII has no decomposition or equivalence expansion.
            if ch.is_ascii() {
                return Some(if info.case_sensitive {
                    ch as u8
                } else {
                    (ch as u8).to_ascii_lowercase()
                });
            }
            self.refill(ch);
        }
    }
}

/// Equality-only companion to [`compare_strings_for_info`]. Predicates such
/// as `IN (...)` do not need ordering, so their common all-ASCII path can
/// stop at the first unequal collation byte without constructing an
/// `Ordering` or entering the Unicode key writer. The fallback remains the
/// comparison oracle for non-ASCII text and UCA 9.0's ignorable NUL.
#[inline]
pub(crate) fn strings_equal_for_info(info: &CollationInfo, left: &str, right: &str) -> bool {
    let left = trim_for_pad(info, left);
    let right = trim_for_pad(info, right);
    if info.binary {
        return left.as_bytes() == right.as_bytes();
    }
    if info.id == UTF8MB4_0900_AI_CI_ID {
        return uca9::compare(left, right) == Ordering::Equal;
    }
    if let Some(equal) = ascii_eq_fused(
        left.as_bytes(),
        right.as_bytes(),
        info.case_sensitive,
        is_uca_0900_collation(info),
    ) {
        return equal;
    }
    compare_strings_for_info(info, left, right) == Ordering::Equal
}

/// Whether `value` has the same collation key as the empty string.
///
/// This is the common `text = ''` / `text <> ''` predicate kernel. It mirrors
/// [`write_collation_key_for_info`] without materializing the key: PAD SPACE
/// trimming happens first, UCA 9.0 NULs are primary-ignorable, and
/// accent-insensitive collations discard compatibility-decomposed combining
/// marks. Case folding and tailoring can change non-empty weights but cannot
/// turn a remaining base character into an empty key.
#[inline]
pub(crate) fn string_equals_empty_for_info(info: &CollationInfo, value: &str) -> bool {
    let value = trim_for_pad(info, value);
    if value.is_empty() {
        return true;
    }
    if info.binary || matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        return false;
    }
    if info.id == UTF8MB4_0900_AI_CI_ID {
        return uca9::weights(value).next().is_none();
    }
    let ignore_nul = is_uca_0900_collation(info);
    if info.accent_sensitive {
        return ignore_nul && value.as_bytes().iter().all(|byte| *byte == 0);
    }
    value
        .nfkd()
        .all(|ch| is_combining_mark(ch) || (ignore_nul && ch == '\0'))
}

#[inline]
fn ascii_eq_fused(a: &[u8], b: &[u8], case_sensitive: bool, ignore_nul: bool) -> Option<bool> {
    let common = a.len().min(b.len());
    for index in 0..common {
        let left = a[index];
        let right = b[index];
        if left | right >= 0x80 || (ignore_nul && (left == 0 || right == 0)) {
            return None;
        }
        let (left, right) = if case_sensitive {
            (left, right)
        } else {
            (left.to_ascii_lowercase(), right.to_ascii_lowercase())
        };
        if left != right {
            return Some(false);
        }
    }
    if a.len() == b.len() {
        return Some(true);
    }
    let tail = if a.len() > b.len() {
        a[common]
    } else {
        b[common]
    };
    if tail >= 0x80 || (ignore_nul && tail == 0) {
        None
    } else {
        Some(false)
    }
}

/// Single-pass ASCII collation compare. Returns the ordering as long as
/// every byte inspected before the decision point is ASCII; returns
/// `None` (caller takes the Unicode collation-key path) the moment a
/// non-ASCII byte could influence the result. Decision rules:
///
/// * first differing byte, both ASCII → that byte pair decides
///   (lowercased for CI collations);
/// * common prefix equal and all-ASCII, equal lengths → `Equal`;
/// * common prefix equal, the longer side continues with an ASCII byte
///   → the longer side is greater (an ASCII char never normalizes away,
///   so the Unicode path would reach the same answer);
/// * any non-ASCII byte at or before the decision point → `None`.
///
/// Walks 8-byte words and skips equal high-bit-free words in one
/// comparison, so long equal prefixes stay near `memcmp` speed instead
/// of degrading to the per-byte loop.
fn ascii_cmp_fused(a: &[u8], b: &[u8], case_sensitive: bool, ignore_nul: bool) -> Option<Ordering> {
    const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
    #[inline]
    fn has_zero_byte(word: u64) -> bool {
        const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
        const LOW_BITS: u64 = 0x0101_0101_0101_0101;
        word.wrapping_sub(LOW_BITS) & !word & HIGH_BITS != 0
    }
    let common = a.len().min(b.len());
    let mut i = 0;
    while i + 8 <= common {
        let wa = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
        let wb = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        if (wa | wb) & HIGH_BITS != 0 {
            break; // word holds a non-ASCII byte: settle per byte below
        }
        if ignore_nul && (has_zero_byte(wa) || has_zero_byte(wb)) {
            return ascii_cmp_ignoring_nul(&a[i..], &b[i..], case_sensitive);
        }
        if wa != wb && case_sensitive {
            // Case-sensitive: the raw bytes decide; find the first
            // differing byte inside this word.
            let diff = (wa ^ wb).trailing_zeros() as usize / 8;
            return Some(a[i + diff].cmp(&b[i + diff]));
        }
        if wa != wb {
            // CI: a raw difference may still be a case fold; settle
            // this word per byte.
            for j in i..i + 8 {
                let la = a[j].to_ascii_lowercase();
                let lb = b[j].to_ascii_lowercase();
                if la != lb {
                    return Some(la.cmp(&lb));
                }
            }
        }
        i += 8;
    }
    while i < common {
        let (x, y) = (a[i], b[i]);
        if x | y >= 0x80 {
            return None;
        }
        if ignore_nul && (x == 0 || y == 0) {
            return ascii_cmp_ignoring_nul(&a[i..], &b[i..], case_sensitive);
        }
        let (lx, ly) = if case_sensitive {
            (x, y)
        } else {
            (x.to_ascii_lowercase(), y.to_ascii_lowercase())
        };
        if lx != ly {
            return Some(lx.cmp(&ly));
        }
        i += 1;
    }
    if a.len() == b.len() {
        return Some(Ordering::Equal);
    }
    if ignore_nul {
        // The common prefix contained no NUL, but the longer suffix may be
        // made entirely of ignorable bytes (`"a\0"` versus `"a"`).
        return ascii_cmp_ignoring_nul(&a[i..], &b[i..], case_sensitive);
    }
    // Prefix equal: the longer side decides, provided its next char is
    // ASCII (a non-ASCII continuation needs collation-key semantics —
    // e.g. it could begin with a combining mark).
    let tail_first = if a.len() > b.len() {
        a[common]
    } else {
        b[common]
    };
    if tail_first >= 0x80 {
        return None;
    }
    Some(a.len().cmp(&b.len()))
}

/// Continue an ASCII comparison after the first UCA-9.0-ignorable NUL.
/// Independent cursors are required because skipping a byte on one side can
/// shift every subsequent aligned word.  This path is cold for normal SQL
/// text; keeping it separate leaves the no-NUL loop vector-friendly.
fn ascii_cmp_ignoring_nul(a: &[u8], b: &[u8], case_sensitive: bool) -> Option<Ordering> {
    let (mut ai, mut bi) = (0usize, 0usize);
    loop {
        while a.get(ai) == Some(&0) {
            ai += 1;
        }
        while b.get(bi) == Some(&0) {
            bi += 1;
        }
        match (a.get(ai).copied(), b.get(bi).copied()) {
            (None, None) => return Some(Ordering::Equal),
            (Some(x), None) => {
                return (x < 0x80).then_some(Ordering::Greater);
            }
            (None, Some(y)) => {
                return (y < 0x80).then_some(Ordering::Less);
            }
            (Some(x), Some(y)) => {
                if x | y >= 0x80 {
                    return None;
                }
                let (left, right) = if case_sensitive {
                    (x, y)
                } else {
                    (x.to_ascii_lowercase(), y.to_ascii_lowercase())
                };
                match left.cmp(&right) {
                    Ordering::Equal => {
                        ai += 1;
                        bi += 1;
                    }
                    ordering => return Some(ordering),
                }
            }
        }
    }
}

#[inline]
pub fn default_compare_strings(left: &str, right: &str) -> Ordering {
    compare_strings_for_info(default_collation_info(), left, right)
}

pub fn collation_key(collation: &str, value: &str) -> Vec<u8> {
    let info = resolve_collation(collation)
        .ok()
        .unwrap_or_else(|| default_collation_info());
    collation_key_for_info_bytes(info, value)
}

pub fn collation_key_by_id(collation_id: u16, value: &str) -> Vec<u8> {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    collation_key_for_info_bytes(info, value)
}

#[cfg(test)]
/// Write `value`'s collation key into `out`, replacing whatever it
/// previously held. Equivalent in output to
/// `*out = collation_key(collation, value)` but avoids the throwaway
/// `Vec<u8>` that the previous implementation produced — every caller
/// reuses one buffer.
pub fn collation_key_into(collation: &str, value: &str, out: &mut Vec<u8>) {
    let info = resolve_collation(collation)
        .ok()
        .unwrap_or_else(|| default_collation_info());
    out.clear();
    write_collation_key_for_info(info, value, out);
}

/// Identical to [`collation_key_into`] but resolves by id (avoids any
/// HashMap query on the caller-side hot path — id → info is a single
/// dense array lookup).
pub fn collation_key_by_id_into(collation_id: u16, value: &str, out: &mut Vec<u8>) {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    out.clear();
    write_collation_key_for_info(info, value, out);
}

/// Identical to [`collation_key_by_id_into`] when the caller already owns the
/// resolved collation. Long-running columnar loops can resolve schema metadata
/// once instead of repeating the dense id lookup for every value.
#[inline]
pub(crate) fn collation_key_for_info_into(info: &CollationInfo, value: &str, out: &mut Vec<u8>) {
    out.clear();
    write_collation_key_for_info(info, value, out);
}

/// Append at most `max_bytes` bytes from the beginning of `value`'s canonical
/// collation key.  This is intended for conservative sketches: equal strings
/// always have equal prefixes, while unequal strings are allowed to collide.
///
/// Accent-insensitive keys can be streamed character by character because
/// combining marks are discarded. Canonical combining-order changes are
/// therefore unobservable, which lets callers stop after a short prefix
/// instead of normalizing a long value in full. Other collations use the
/// canonical writer as their correctness fallback.
pub(crate) fn collation_key_prefix_by_id_into(
    collation_id: u16,
    value: &str,
    max_bytes: usize,
    out: &mut Vec<u8>,
) {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    let value = trim_for_pad(info, value);
    let start = out.len();
    if max_bytes == 0 {
        return;
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        out.extend(
            value
                .chars()
                .flat_map(|ch| legacy_general::primary(ch).to_be_bytes())
                .take(max_bytes),
        );
        return;
    }
    if info.binary {
        out.extend_from_slice(&value.as_bytes()[..value.len().min(max_bytes)]);
        return;
    }
    if info.id == UTF8MB4_0900_AI_CI_ID {
        uca9::key_prefix_into(value, max_bytes, out);
        return;
    }
    // A Unicode suffix cannot change an already-ASCII collation prefix.
    // Inspect only the requested window, then fold it as one contiguous lane
    // instead of decoding/pushing one character at a time.
    let prefix = &value.as_bytes()[..value.len().min(max_bytes)];
    if prefix.is_ascii() && (!is_uca_0900_collation(info) || !prefix.contains(&0)) {
        let begin = out.len();
        out.extend_from_slice(prefix);
        if !info.case_sensitive {
            lowercase_ascii_lane(&mut out[begin..]);
        }
        return;
    }
    if value.is_ascii() {
        for byte in value.bytes() {
            if is_uca_0900_collation(info) && byte == 0 {
                continue;
            }
            out.push(if info.case_sensitive {
                byte
            } else {
                byte.to_ascii_lowercase()
            });
            if out.len() - start == max_bytes {
                break;
            }
        }
        return;
    }
    if info.accent_sensitive {
        let mut full = Vec::with_capacity(value.len());
        write_collation_key_unicode(info, value, &mut full);
        out.extend_from_slice(&full[..full.len().min(max_bytes)]);
        return;
    }

    let ignore_nul = is_uca_0900_collation(info);
    'input: for ch in value.chars() {
        if ignore_nul && ch == '\0' {
            continue;
        }
        let (tailored, tailored_len) = if info.german_phonebook {
            german_phonebook_expansion(ch)
        } else {
            ([ch, '\0'], 1)
        };
        for tailored in tailored.into_iter().take(tailored_len) {
            decompose_compatible(tailored, |normalized| {
                if out.len() - start >= max_bytes || is_combining_mark(normalized) {
                    return;
                }
                let mut emit = |bytes: &[u8]| {
                    let remaining = max_bytes - (out.len() - start);
                    out.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
                };
                emit_collation_case_char(info, normalized, info.common_equivalences, &mut emit);
            });
            if out.len() - start >= max_bytes {
                break 'input;
            }
        }
    }
}

fn lowercase_ascii_lane(bytes: &mut [u8]) {
    #[cfg(target_arch = "x86_64")]
    if bytes.len() >= 32 && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the caller proved ASCII, runtime detection proved AVX2.
        unsafe { lowercase_ascii_lane_avx2(bytes) };
        return;
    }
    bytes.make_ascii_lowercase();
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn lowercase_ascii_lane_avx2(bytes: &mut [u8]) {
    use std::arch::x86_64::*;
    let mut chunks = bytes.chunks_exact_mut(32);
    for chunk in chunks.by_ref() {
        let value = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        let uppercase = _mm256_and_si256(
            _mm256_cmpgt_epi8(value, _mm256_set1_epi8((b'A' - 1) as i8)),
            _mm256_cmpgt_epi8(_mm256_set1_epi8((b'Z' + 1) as i8), value),
        );
        let lower = _mm256_or_si256(value, _mm256_and_si256(uppercase, _mm256_set1_epi8(32)));
        unsafe { _mm256_storeu_si256(chunk.as_mut_ptr().cast(), lower) };
    }
    chunks.into_remainder().make_ascii_lowercase();
}

/// Write comparison weights for substring-search functions.
///
/// Equality and sort keys apply PAD SPACE trimming. A LOCATE needle must keep
/// its spaces, so substring search uses the same collation transforms without
/// trimming either edge.
pub fn collation_search_key_by_id_into(collation_id: u16, value: &str, out: &mut Vec<u8>) {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    out.clear();
    if info.binary {
        out.extend_from_slice(value.as_bytes());
    } else {
        write_collation_key_unicode_or_ascii(info, value, out);
    }
}

/// Materialise `value`'s collation key into a thread-local scratch
/// buffer and hand it to `f`. The callback owns the borrow for the
/// duration of `f`; the buffer is never returned to the caller, so
/// nested calls on the same thread are forbidden — `f` must not call
/// any `with_collation_key_*` API itself. Returns whatever `f` returns.
///
/// This is the zero-allocation form of `collation_key_by_id` used by
/// hot paths that only need to *consume* the bytes once (B-tree memcmp
/// encoding, hash digest, statistics digest). They no longer allocate
/// one throwaway `Vec<u8>` per row.
#[inline]
pub fn with_collation_key_by_id<R>(
    collation_id: u16,
    value: &str,
    f: impl FnOnce(&[u8]) -> R,
) -> R {
    let info = resolve_collation_id(collation_id).unwrap_or_else(|| default_collation_info());
    with_collation_key_for_info(info, value, f)
}

/// Losslessly pack a collation key of at most six bytes into a compact lane
/// (`len` in the low four bits, payload above it). ASCII inputs use the same
/// PAD SPACE/case/NUL rules as the canonical writer without entering the
/// thread-local scratch buffer; non-ASCII inputs reuse that writer and are
/// accepted only when their complete key still fits.
/// Pack a short key with an already-resolved collation. Operators that encode
/// millions of values under one schema resolve the collation once when they
/// compile their key layout and use this entry in the hot loop.
pub(crate) fn pack_short_collation_key_for_info(info: &CollationInfo, value: &str) -> Option<u128> {
    let value = trim_for_pad(info, value);
    let pack = |bytes: &[u8]| {
        if bytes.len() > 6 {
            return None;
        }
        let mut packed = bytes.len() as u128;
        for (index, byte) in bytes.iter().enumerate() {
            packed |= u128::from(*byte) << (4 + index * 8);
        }
        Some(packed)
    };
    if info.id == UTF8MB4_0900_AI_CI_ID {
        let mut bytes = smallvec::SmallVec::<[u8; 6]>::new();
        for byte in uca9::key_bytes(value) {
            if bytes.len() == 6 {
                return None;
            }
            bytes.push(byte);
        }
        return pack(&bytes);
    }
    if value.is_ascii() {
        let mut bytes = [0u8; 6];
        let mut len = 0usize;
        for mut byte in value.bytes() {
            if byte == 0 && is_uca_0900_collation(info) && !info.binary {
                continue;
            }
            if !info.binary && !info.case_sensitive {
                byte = byte.to_ascii_lowercase();
            }
            if len == bytes.len() {
                return None;
            }
            bytes[len] = byte;
            len += 1;
        }
        return pack(&bytes[..len]);
    }
    with_collation_key_for_info(info, value, pack)
}

/// Same as [`with_collation_key_by_id`] but for the default collation.
#[inline]
pub fn with_default_string_key<R>(value: &str, f: impl FnOnce(&[u8]) -> R) -> R {
    with_collation_key_for_info(default_collation_info(), value, f)
}

#[inline]
fn with_collation_key_for_info<R>(
    info: &CollationInfo,
    value: &str,
    f: impl FnOnce(&[u8]) -> R,
) -> R {
    COLLATION_KEY_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        write_collation_key_for_info(info, value, &mut buf);
        f(buf.as_slice())
    })
}

fn collation_key_for_info_bytes(info: &CollationInfo, value: &str) -> Vec<u8> {
    let value = trim_for_pad(info, value);
    if info.binary {
        return value.as_bytes().to_vec();
    }
    // Reuses the shared writer so the public `Vec<u8>` form stays in
    // lock-step with the zero-alloc paths; semantics are identical.
    let mut out = Vec::with_capacity(value.len());
    write_collation_key_unicode_or_ascii(info, value, &mut out);
    out
}

/// Exact scratch and escaped-key sizes for the ASCII encoded-sort path.
/// UCA primary weights are binary units; their size is independent of UTF-8.
pub(crate) fn ascii_collation_sort_key_size(id: u16, text: &str) -> (usize, usize) {
    debug_assert!(text.is_ascii());
    let info = resolve_collation_id(id).unwrap_or_else(default_collation_info);
    let text = trim_for_pad(info, text);
    let mut length = 0;
    let mut zeros = 0;
    let mut count = |byte| {
        length += 1;
        zeros += usize::from(byte == 0);
    };
    if info.id == UTF8MB4_0900_AI_CI_ID {
        return (uca9::ascii_key_size(text), 0);
    } else if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        for byte in text
            .chars()
            .flat_map(|ch| legacy_general::primary(ch).to_be_bytes())
        {
            count(byte);
        }
    } else {
        for byte in text.bytes() {
            if is_uca_0900_collation(info) && byte == 0 {
                continue;
            }
            count(byte);
        }
    }
    (length, zeros)
}

pub(crate) fn uca9_prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = uca9::scalar_successor(last) {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

pub fn default_string_key(value: &str) -> Vec<u8> {
    let info = default_collation_info();
    collation_key_for_info_bytes(info, value)
}

/// Zero-allocation variant: writes the default-collation key for
/// `value` into `out`, replacing prior contents. Used by `ValueKey`
/// construction on the GROUP BY / DISTINCT / JOIN hot path.
pub fn default_string_key_into(value: &str, out: &mut Vec<u8>) {
    out.clear();
    write_collation_key_for_info(default_collation_info(), value, out);
}

#[path = "charset/legacy_general.rs"]
mod legacy_general;

pub fn weight_string(value: &str, collation: Option<&str>) -> Vec<u8> {
    let info = resolve_collation(collation.unwrap_or(DEFAULT_COLLATION))
        .unwrap_or_else(|_| default_collation_info());
    if info.name == "utf8mb4_0900_ai_ci" {
        return uca9::primary(value);
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        return value
            .chars()
            .flat_map(|ch| legacy_general::primary(ch).to_be_bytes())
            .collect();
    }
    if info.binary {
        if info.charset.starts_with("utf8") && !info.name.contains("0900") {
            return value
                .chars()
                .flat_map(|c| {
                    let bytes = (c as u32).to_be_bytes();
                    [bytes[1], bytes[2], bytes[3]]
                })
                .collect();
        }
        return value.as_bytes().to_vec();
    }
    if matches!(info.name, "latin2_czech_cs" | "cp1250_czech_cs") {
        if let Some(weight) = czech_ascii_weight_string(value) {
            return weight;
        }
    }
    // WEIGHT_STRING exposes MySQL's sortable weight bytes, not our private
    // normalized comparison key. Legacy single-byte collations use uppercase
    // primary ASCII weights (latin1_swedish_ci: 'a' => 0x41), and an explicit
    // AS CHAR(N) pad must remain observable as 0x20 instead of being removed
    // by PAD SPACE comparison normalization.
    if value.is_ascii() {
        if info.case_sensitive {
            return value.as_bytes().to_vec();
        }
        return value
            .as_bytes()
            .iter()
            .map(|byte| byte.to_ascii_uppercase())
            .collect();
    }
    let mut out = Vec::with_capacity(value.len());
    write_collation_key_unicode_or_ascii(info, value, &mut out);
    out
}

pub(crate) fn uca9_primary_weights_into(value: &str, out: &mut Vec<u8>) {
    out.clear();
    uca9::primary_into(value, out);
}

/// A tight byte bound for UCA 9 primary weights. Almost every DUCET entry
/// expands by at most four bytes per UTF-8 byte; the four larger expansions
/// are counted explicitly. Packed-table tests audit this complete bound.
pub(crate) fn uca9_primary_byte_bound(value: &[u8]) -> Option<usize> {
    let mut bytes = value.len().checked_mul(4)?;
    if memchr::memchr2(0xe3, 0xef, value).is_none() {
        return Some(bytes);
    }
    for (scalar, extra) in uca9::PRIMARY_EXPANSION_EXCEPTIONS {
        let mut utf8 = [0; 4];
        let count =
            memchr::memmem::find_iter(value, scalar.encode_utf8(&mut utf8).as_bytes()).count();
        bytes = bytes.checked_add(count.checked_mul(*extra)?)?;
    }
    Some(bytes)
}

/// Four-level Czech sort key used by MySQL's legacy Czech collations.
///
/// Czech has a contraction (`ch`) and cannot be represented by the
/// one-byte case-folding table used by the other legacy collations. Keep the
/// common ASCII alphabet on the real four-level ordering; non-ASCII input
/// continues through the Unicode collation path below until the complete
/// single-byte repertoire is needed. The representation includes a `0x01`
/// level separator and a final `0x00`, exactly like MySQL `strnxfrm`.
fn czech_ascii_weight_string(value: &str) -> Option<Vec<u8>> {
    if !value.is_ascii() || !value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        return None;
    }

    const PRIMARY: [u8; 26] = [
        3, 4, 5, 7, 8, 9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 25, 27, 28, 29, 30, 31,
        32, 33,
    ];
    const SECONDARY: [u8; 26] = [
        3, 9, 10, 14, 17, 22, 23, 24, 26, 29, 30, 31, 35, 36, 39, 44, 45, 46, 49, 53, 56, 61, 62,
        63, 64, 66,
    ];
    const TERTIARY_UPPER: [u8; 26] = [
        4, 16, 18, 26, 32, 42, 44, 46, 51, 57, 59, 61, 69, 71, 77, 87, 89, 91, 97, 105, 111, 121,
        123, 125, 127, 131,
    ];

    fn emit_level(value: &[u8], level: usize, out: &mut Vec<u8>) {
        let mut index = 0;
        while index < value.len() {
            let current = value[index];
            if (current == b'c' || current == b'C')
                && value
                    .get(index + 1)
                    .is_some_and(|next| *next == b'h' || *next == b'H')
            {
                let next = value[index + 1];
                out.push(match level {
                    0 => 12,
                    1 => 25,
                    2 | 3 if current.is_ascii_lowercase() && next.is_ascii_lowercase() => 47,
                    2 | 3 if current.is_ascii_uppercase() && next.is_ascii_uppercase() => 49,
                    2 | 3 => 48,
                    _ => unreachable!(),
                });
                index += 2;
                continue;
            }
            let alphabet_index = usize::from(current.to_ascii_uppercase() - b'A');
            out.push(match level {
                0 => PRIMARY[alphabet_index],
                1 => SECONDARY[alphabet_index],
                2 | 3 => TERTIARY_UPPER[alphabet_index] - u8::from(current.is_ascii_lowercase()),
                _ => unreachable!(),
            });
            index += 1;
        }
    }

    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(4).saturating_add(4));
    for level in 0..4 {
        emit_level(bytes, level, &mut out);
        out.push(if level == 3 { 0 } else { 1 });
    }
    Some(out)
}

#[inline]
fn trim_for_pad<'a>(info: &CollationInfo, value: &'a str) -> &'a str {
    if info.pad_space {
        crate::util::pad_space::pad_space_trim_end_str(value)
    } else {
        value
    }
}

#[inline]
fn is_uca_0900_collation(info: &CollationInfo) -> bool {
    info.uca_0900 && !info.binary
}

/// Single writer that powers every collation-key shape we expose:
/// `collation_key`, `collation_key_into`, `with_collation_key_*`,
/// `compare_strings_for_info`. Takes a pre-padded `value` (PAD SPACE
/// trim already applied by the caller).
///
/// ASCII fast path is identical to the previous one: for every
/// collation in the table NFD on ASCII is identity, `char::to_lowercase`
/// agrees with `to_ascii_lowercase`, and `expand_common_equivalences`
/// rewrites only non-ASCII chars. So ASCII inputs avoid the
/// Unicode-iterator allocations entirely; non-ASCII falls through to
/// the general path that produces an identical key (validated by the
/// existing `ascii_fast_path_matches_unicode_path_*` tests).
#[inline]
fn write_collation_key_unicode_or_ascii(info: &CollationInfo, value: &str, out: &mut Vec<u8>) {
    if info.id == UTF8MB4_0900_AI_CI_ID {
        uca9::key_into(value, out);
        return;
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        out.extend(
            value
                .chars()
                .flat_map(|ch| legacy_general::primary(ch).to_be_bytes()),
        );
        return;
    }
    if value.is_ascii() {
        let start = out.len();
        out.reserve(value.len());
        if is_uca_0900_collation(info) && value.as_bytes().contains(&0) {
            out.extend(value.bytes().filter(|byte| *byte != 0));
        } else {
            out.extend_from_slice(value.as_bytes());
        }
        if !info.case_sensitive {
            lowercase_ascii_lane(&mut out[start..]);
        }
        return;
    }
    write_collation_key_unicode(info, value, out);
}

/// Shared entry point — handles `info.binary` (raw bytes) before
/// falling through to the ASCII/Unicode writer. Mirrors the contract
/// of `collation_key_for_info_bytes` so the two stay in lock-step.
#[inline]
fn write_collation_key_for_info(info: &CollationInfo, value: &str, out: &mut Vec<u8>) {
    let value = trim_for_pad(info, value);
    if info.binary {
        out.extend_from_slice(value.as_bytes());
        return;
    }
    write_collation_key_unicode_or_ascii(info, value, out);
}

fn write_collation_key_unicode(info: &CollationInfo, value: &str, out: &mut Vec<u8>) {
    if info.id == UTF8MB4_0900_AI_CI_ID {
        uca9::key_into(value, out);
        return;
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        out.extend(
            value
                .chars()
                .flat_map(|ch| legacy_general::primary(ch).to_be_bytes()),
        );
        return;
    }
    let ignore_nul = is_uca_0900_collation(info);
    out.reserve(value.len());
    // Most non-ASCII database text (CJK, Cyrillic and already-normalized
    // UTF-8) is already in NFKD form. In that case compatibility
    // decomposition and combining-mark removal are identities, so stream the
    // same case/equivalence transformation directly. This keeps accented and
    // compatibility characters on the exact oracle below while avoiding the
    // decomposition iterator and two Unicode table lookups per character for
    // the common normalized case.
    if !info.accent_sensitive && is_nfkd_quick(value.chars()) == IsNormalized::Yes {
        for ch in value.chars() {
            if ch.is_ascii() {
                let byte = ch as u8;
                if !ignore_nul || byte != 0 {
                    out.push(if info.case_sensitive {
                        byte
                    } else {
                        byte.to_ascii_lowercase()
                    });
                }
                continue;
            }
            if (!ignore_nul || ch != '\0') && !is_combining_mark(ch) {
                write_collation_case_char(info, ch, info.common_equivalences, out);
            }
        }
        return;
    }
    for ch in value.chars() {
        // ASCII compatibility decomposition is identity, ASCII has no
        // combining marks/common-equivalence expansions, and Unicode case
        // folding agrees with `to_ascii_lowercase`. Mixed strings (URLs,
        // paths, identifiers with one non-ASCII component) therefore keep the
        // exact Unicode oracle bytes without routing their long ASCII spans
        // through the per-char normalization iterator.
        if ch.is_ascii() {
            let byte = ch as u8;
            if !ignore_nul || byte != 0 {
                out.push(if info.case_sensitive {
                    byte
                } else {
                    byte.to_ascii_lowercase()
                });
            }
            continue;
        }
        if ignore_nul && ch == '\0' {
            continue;
        }
        let (tailored, tailored_len) = if info.german_phonebook {
            german_phonebook_expansion(ch)
        } else {
            ([ch, '\0'], 1)
        };
        for tailored in tailored.into_iter().take(tailored_len) {
            if info.accent_sensitive {
                write_collation_case_char(info, tailored, false, out);
            } else {
                for normalized in std::iter::once(tailored).nfkd() {
                    if !is_combining_mark(normalized) {
                        write_collation_case_char(info, normalized, info.common_equivalences, out);
                    }
                }
            }
        }
    }
}

#[inline]
fn german_phonebook_expansion(ch: char) -> ([char; 2], usize) {
    match ch {
        'ä' => (['a', 'e'], 2),
        'Ä' => (['A', 'e'], 2),
        'ö' => (['o', 'e'], 2),
        'Ö' => (['O', 'e'], 2),
        'ü' => (['u', 'e'], 2),
        'Ü' => (['U', 'e'], 2),
        'ß' => (['s', 's'], 2),
        _ => ([ch, '\0'], 1),
    }
}

#[inline]
fn write_collation_case_char(
    info: &CollationInfo,
    ch: char,
    common_equivalences: bool,
    out: &mut Vec<u8>,
) {
    if info.case_sensitive {
        write_collation_weight_char(ch, common_equivalences, out);
    } else {
        for folded in ch.to_lowercase() {
            write_collation_weight_char(folded, common_equivalences, out);
        }
    }
}

#[inline]
fn emit_collation_case_char(
    info: &CollationInfo,
    ch: char,
    common_equivalences: bool,
    emit: &mut impl FnMut(&[u8]),
) {
    let mut emit_weight = |ch: char| {
        if common_equivalences {
            if let Some(replacement) = common_equivalence(ch) {
                emit(replacement);
                return;
            }
        }
        let mut buf = [0u8; 4];
        emit(ch.encode_utf8(&mut buf).as_bytes());
    };
    if info.case_sensitive {
        emit_weight(ch);
    } else {
        for folded in ch.to_lowercase() {
            emit_weight(folded);
        }
    }
}

#[inline]
fn write_collation_weight_char(ch: char, common_equivalences: bool, out: &mut Vec<u8>) {
    if common_equivalences {
        if let Some(replacement) = common_equivalence(ch) {
            out.extend_from_slice(replacement);
            return;
        }
    }
    let mut buf = [0_u8; 4];
    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
}

#[cfg(test)]
fn collation_key_ascii(info: &CollationInfo, value: &str) -> Vec<u8> {
    assert!(value.is_ascii(), "ASCII-path oracle requires ASCII input");
    let mut out = Vec::with_capacity(value.len());
    write_collation_key_unicode_or_ascii(info, value, &mut out);
    out
}

#[cfg(test)]
fn collation_key_unicode(info: &CollationInfo, value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len());
    write_collation_key_unicode(info, value, &mut out);
    // safety: every transformation we apply (NFD, case folding,
    // common-equivalence expansion) maps valid UTF-8 to valid UTF-8.
    out
}

#[cfg(test)]
fn strip_accents(value: &str) -> String {
    // UCA primary weights include compatibility decomposition. In
    // particular, U+2167 ROMAN NUMERAL EIGHT searches like "viii".
    value.nfkd().filter(|ch| !is_combining_mark(*ch)).collect()
}

#[cfg(test)]
fn write_collation_key_unicode_reference(info: &CollationInfo, value: &str, out: &mut Vec<u8>) {
    if info.id == UTF8MB4_0900_AI_CI_ID {
        uca9::key_into(value, out);
        return;
    }
    if matches!(info.name, "utf8mb4_general_ci" | "utf8mb3_general_ci") {
        out.extend(weight_string(value, Some(info.name)));
        return;
    }
    let without_primary_ignorables = is_uca_0900_collation(info)
        .then(|| value.chars().filter(|ch| *ch != '\0').collect::<String>());
    let value = without_primary_ignorables.as_deref().unwrap_or(value);
    let german_phonebook =
        (info.name.contains("german2") || info.name.contains("de_pb_")).then(|| {
            let mut tailored = String::with_capacity(value.len());
            for ch in value.chars() {
                let (expanded, len) = german_phonebook_expansion(ch);
                for expanded in expanded.into_iter().take(len) {
                    tailored.push(expanded);
                }
            }
            tailored
        });
    let value = german_phonebook.as_deref().unwrap_or(value);
    let normalized: Cow<'_, str> = if info.accent_sensitive {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(strip_accents(value))
    };
    let cased: Cow<'_, str> = if info.case_sensitive {
        normalized
    } else {
        Cow::Owned(normalized.chars().flat_map(char::to_lowercase).collect())
    };
    if info.accent_sensitive {
        out.extend_from_slice(cased.as_bytes());
    } else if is_uca_0900_collation(info)
        || info.name.contains("_unicode_")
        || info.name.contains("german2")
        || info.name.contains("de_pb_")
    {
        write_common_equivalences(&cased, out);
    } else {
        out.extend_from_slice(cased.as_bytes());
    }
}

#[cfg(test)]
fn write_common_equivalences(value: &str, out: &mut Vec<u8>) {
    out.reserve(value.len());
    for ch in value.chars() {
        let replacement = common_equivalence(ch);
        match replacement {
            Some(bytes) => out.extend_from_slice(bytes),
            None => {
                let mut tmp = [0u8; 4];
                let encoded = ch.encode_utf8(&mut tmp);
                out.extend_from_slice(encoded.as_bytes());
            }
        }
    }
}

#[inline]
fn common_equivalence(ch: char) -> Option<&'static [u8]> {
    match ch {
        'ß' => Some(b"ss"),
        'æ' | 'Æ' => Some(b"ae"),
        'œ' | 'Œ' => Some(b"oe"),
        'þ' | 'Þ' => Some(b"th"),
        'ð' | 'Ð' => Some(b"d"),
        'ł' | 'Ł' => Some(b"l"),
        'ø' | 'Ø' => Some(b"o"),
        _ => None,
    }
}

fn incorrect_string_value(charset: &str) -> QueryError {
    mysql_errors::error(
        mysql_errors::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
        format!("incorrect string value for character set '{charset}'"),
    )
}

fn unknown_charset(name: &str) -> QueryError {
    mysql_errors::error(
        mysql_errors::ER_UNKNOWN_CHARACTER_SET,
        format!("unknown character set: '{name}'"),
    )
}

fn unknown_collation(name: &str) -> QueryError {
    mysql_errors::error(
        mysql_errors::ER_UNKNOWN_COLLATION,
        format!("unknown collation: '{name}'"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_mysql80_charset_surface() {
        assert_eq!(CHARSETS.len(), 41);
        assert_eq!(resolve_charset("utf8").unwrap().name, "utf8mb3");
        assert_eq!(
            default_collation_for_charset("utf8mb4").unwrap(),
            "utf8mb4_0900_ai_ci"
        );
        assert_eq!(
            default_collation_for_charset("utf8mb3").unwrap(),
            "utf8mb3_general_ci"
        );
        assert!(!resolve_charset("ucs2").unwrap().client_allowed);
        assert!(resolve_charset("gb18030").unwrap().client_allowed);
    }

    #[test]
    fn exposes_mysql80_collation_surface() {
        assert!(collations().len() > 250);
        assert_eq!(
            resolve_collation("utf8_unicode_ci").unwrap().name,
            "utf8mb3_unicode_ci"
        );
        assert_eq!(resolve_collation("utf8mb4_0900_bin").unwrap().id, 309);
        assert_eq!(
            resolve_collation("utf8mb4_0900_ai_ci")
                .unwrap()
                .pad_attribute,
            "NO PAD"
        );
        assert_eq!(collation_name_for_id(255), "utf8mb4_0900_ai_ci");
    }

    #[test]
    fn validates_client_charset_restrictions() {
        assert_eq!(resolve_client_charset("utf8mb4").unwrap().name, "utf8mb4");
        assert_eq!(resolve_client_charset("utf16").unwrap_err().code(), 1231);
    }

    #[test]
    fn validates_representative_encodings() {
        validate_text_for_charset("ascii", "abc").unwrap();
        assert_eq!(
            validate_text_for_charset("ascii", "é").unwrap_err().code(),
            1366
        );
        validate_text_for_charset("latin1", "é").unwrap();
        assert_eq!(
            validate_text_for_charset("latin1", "中")
                .unwrap_err()
                .code(),
            1366
        );
        validate_text_for_charset("gbk", "中文").unwrap();
        validate_text_for_charset("cp932", "日本語").unwrap();
        assert_eq!(
            validate_text_for_charset("utf8mb3", "😀")
                .unwrap_err()
                .code(),
            1366
        );
    }

    #[test]
    fn utf16_ucs2_and_utf32_use_mysql_byte_order() {
        let text = "Aé中😀";
        for charset in ["utf16", "utf16le", "utf32"] {
            let encoded = encode_string(charset, text).unwrap();
            assert_eq!(decode_bytes(charset, &encoded).unwrap(), text, "{charset}");
        }
        assert_eq!(
            encode_string("utf16", "a12\u{7}").unwrap(),
            [0, 0x61, 0, 0x31, 0, 0x32, 0, 0x07]
        );
        assert_eq!(encode_string("utf16le", "A").unwrap(), [0x41, 0x00]);
        assert_eq!(encode_string("utf32", "A").unwrap(), [0, 0, 0, 0x41]);
        assert_eq!(decode_bytes("ucs2", &[0x00, 0xE9]).unwrap(), "é");
        assert_eq!(encode_string("ucs2", "😀").unwrap_err().code(), 1366);
        assert_eq!(decode_bytes("utf16", &[0]).unwrap_err().code(), 1366);
        assert_eq!(
            decode_bytes("utf32", &[0, 0, 0xD8, 0]).unwrap_err().code(),
            1366
        );
    }

    #[test]
    fn default_0900_ai_ci_is_no_pad_case_and_accent_insensitive() {
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "A", "a"),
            Ordering::Equal
        );
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "e", "é"),
            Ordering::Equal
        );
        assert_ne!(
            compare_strings(DEFAULT_COLLATION, "a", "a "),
            Ordering::Equal
        );
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "ß", "ss"),
            Ordering::Equal
        );
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "a\0", "a"),
            Ordering::Equal,
            "U+0000 is primary-ignorable in UCA 9.0"
        );
        assert_eq!(
            collation_key(DEFAULT_COLLATION, "a\0"),
            collation_key(DEFAULT_COLLATION, "a"),
            "comparison and materialized index/hash keys must agree"
        );
        assert_eq!(
            compare_strings("utf8mb4_0900_bin", "a\0", "a"),
            Ordering::Greater,
            "binary collations retain the NUL byte"
        );
    }

    #[test]
    fn empty_key_predicate_matches_comparison_oracle_for_all_collations() {
        let inputs = [
            "",
            " ",
            "   ",
            "a",
            "A",
            "\0",
            "\0\0",
            "\u{0301}",
            "\u{0301}\u{0308}",
            "é",
            "ê",
            "ß",
            "中文",
            "a   ",
        ];
        for info in collations_to_check() {
            for value in inputs {
                assert_eq!(
                    string_equals_empty_for_info(info, value),
                    compare_strings_for_info(info, value, "") == Ordering::Equal,
                    "empty-key predicate drift for collation {} and value {:?}",
                    info.name,
                    value
                );
            }
        }
    }

    #[test]
    fn fused_unicode_key_writer_matches_allocating_reference_for_all_collations() {
        let inputs = [
            "école",
            "E\u{0301}COLE",
            "\u{0301}\u{0327}",
            "a\u{0327}\u{0301}z",
            "äöüß",
            "ÆŒÞÐŁØ",
            "\0café\0",
            "中文 русский 日本語",
            "Ⅷ Straße",
        ];
        for info in collations_to_check() {
            for value in inputs {
                let mut expected = Vec::new();
                write_collation_key_unicode_reference(info, value, &mut expected);
                let mut actual = Vec::new();
                write_collation_key_unicode(info, value, &mut actual);
                assert_eq!(
                    actual, expected,
                    "fused Unicode key drift for collation {} and value {:?}",
                    info.name, value
                );
            }
        }
    }

    #[test]
    fn german_phonebook_collations_expand_umlauts() {
        for (left, right) in [("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss")] {
            assert_eq!(
                compare_strings("latin1_german2_ci", left, right),
                Ordering::Equal,
                "{left} must have the German phonebook weight of {right}"
            );
        }
        assert_ne!(
            compare_strings("latin1_german1_ci", "ä", "ae"),
            Ordering::Equal
        );
    }

    fn collations_to_check() -> Vec<&'static CollationInfo> {
        // Representatives of every flag combination the ASCII fast path
        // needs to handle equivalently.
        [
            "utf8mb4_0900_ai_ci", // accent-insensitive + case-insensitive
            "utf8mb4_general_ci",
            "utf8mb3_general_ci",
            "latin1_general_ci",
            "ascii_general_ci",
            "utf8mb4_0900_as_cs", // accent-sensitive + case-sensitive
            "utf8mb4_0900_as_ci", // accent-sensitive + case-insensitive
            "utf8mb4_bin",        // case-sensitive binary
        ]
        .iter()
        .map(|name| resolve_collation(name).unwrap_or_else(|_| panic!("missing collation {name}")))
        .collect()
    }

    #[test]
    fn lazy_collation_comparison_matches_materialized_keys() {
        let mut samples: Vec<String> = [
            "",
            "a",
            "ABC",
            "abc ",
            "\0",
            "a\0b",
            "ab",
            "é",
            "e\u{301}",
            "\u{301}\u{327}",
            "E\u{327}\u{301}",
            "ß",
            "ss",
            "æ",
            "AE",
            "İ",
            "i\u{307}",
            "ä",
            "ae",
            "ЙЁ",
            "ие",
            "键-001",
            "键-002",
            "中😀文",
            "中😀字",
            "中😀",
            "中😁",
            "中",
            "串",
            "Ⅷ",
            "viii",
            "ﬃ",
            "ffi",
            "\u{FDFA}",
            "\u{1D160}",
            "\u{10FFFF}",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        for n in 0..128 {
            let ch = char::from_u32((n * 7919) % 0x110000).unwrap_or('x');
            samples.push(format!("{ch}aÉ\0{}", "z".repeat((n % 80) as usize)));
        }
        for suffix in ["é", "e\u{301}", "ß", "ss", "ä", "ae", "\0", "\u{301}"] {
            samples.push(format!("{}{suffix}", "中😀".repeat(40)));
        }
        // Every advertised collation must keep its current key ordering,
        // including binary, legacy general, PAD SPACE and phonebook rules.
        for info in collations() {
            let keys: Vec<_> = samples
                .iter()
                .map(|s| collation_key_for_info_bytes(info, s))
                .collect();
            for (i, left) in samples.iter().enumerate() {
                for (j, right) in samples.iter().enumerate() {
                    assert_eq!(
                        compare_strings_for_info(info, left, right),
                        keys[i].cmp(&keys[j]),
                        "{}: {left:?} vs {right:?}",
                        info.name
                    );
                }
            }
        }
    }

    #[test]
    fn collation_input_prefix_matches_byte_oracle_on_unaligned_slices() {
        let bytes: Vec<u8> = (0..256).map(|n| (n * 79) as u8).collect();
        for start in 0..8 {
            for len in 0..128 {
                let left = &bytes[start..start + len];
                for mismatch in 0..=len {
                    let mut right = left.to_vec();
                    if mismatch < len {
                        right[mismatch] ^= 0x80;
                    }
                    for right in [&right[..], &right[..right.len() / 2]] {
                        let expected = left.iter().zip(right).take_while(|(l, r)| l == r).count();
                        assert_eq!(common_collation_input_prefix(left, right), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn lazy_unicode_weights_match_writer_for_all_scalars() {
        // Exercise all Unicode compatibility expansions, case expansions and
        // combining marks, including expansions larger than the inline buffer.
        for name in [
            "utf8mb4_0900_ai_ci",
            "utf8mb4_0900_as_ci",
            "utf8mb4_0900_as_cs",
            "utf8mb4_de_pb_0900_as_cs",
            "latin1_german2_ci",
            "latin1_general_ci",
        ] {
            let info = resolve_collation(name).unwrap();
            let mut expected = Vec::new();
            let mut actual = Vec::new();
            for scalar in 0..=0x10FFFF {
                let Some(ch) = char::from_u32(scalar) else {
                    continue;
                };
                let mut utf8 = [0; 4];
                let value = ch.encode_utf8(&mut utf8);
                expected.clear();
                write_collation_key_unicode_or_ascii(info, value, &mut expected);
                actual.clear();
                actual.extend(CollationWeights::new(info, value));
                assert_eq!(actual, expected, "{name}: U+{scalar:04X}");
            }
        }
    }

    #[test]
    fn unicode_char_to_ascii_like_fast_path_matches_comparison_oracle() {
        let collations = [
            "utf8mb4_0900_ai_ci",
            "utf8mb4_general_ci",
            "utf8mb4_0900_as_ci",
            "utf8mb4_0900_as_cs",
            "latin1_german2_ci",
            "utf8mb4_bin",
        ];
        let values = [
            'é', 'É', 'ö', 'Ö', 'ø', 'Ø', 'ß', 'æ', 'þ', 'ð', 'ł', 'K', 'Ⅷ', '中', '\u{0301}',
        ];
        for name in collations {
            let info = resolve_collation(name).unwrap();
            for value in values {
                for pattern in 0u8..=127 {
                    let expected = like_chars_equal_for_info(info, value, pattern as char);
                    let actual = like_char_equals_ascii_for_info(info, value, pattern);
                    assert_eq!(
                        actual, expected,
                        "collation={name} value={value:?} pattern={pattern:?}"
                    );
                }
            }
        }
    }

    /// The ASCII fast path in `collation_key_for_info` must produce the
    /// exact same key bytes as the general Unicode path for every ASCII
    /// input. Drift here would silently corrupt ORDER BY / DISTINCT /
    /// GROUP BY / equality semantics, so the property is checked across
    /// the flag matrix on a representative input set.
    #[test]
    fn ascii_fast_path_matches_unicode_path_for_all_collations() {
        let samples = [
            "",
            "a",
            "A",
            "abc",
            "ABC",
            "Hello, World!",
            "01666544319-15292904661-62336853652", // sysbench-style payload
            "abc def",
            "ABC def",
            "abc\0def",
            " trailing-space   ",
            "MiXeD CaSe 123",
            "0123456789",
            "!@#$%^&*()_+-={}|[]:;'<>?,./",
        ];
        for info in collations_to_check() {
            for s in samples {
                let ascii = collation_key_ascii(info, s);
                let unicode = collation_key_unicode(info, s);
                assert_eq!(
                    ascii, unicode,
                    "ASCII fast path drift for collation {}, input {:?}: ascii={:?} unicode={:?}",
                    info.name, s, ascii, unicode,
                );
            }
        }
    }

    /// `compare_strings_for_info`'s zero-allocation ASCII compare must
    /// produce the same ordering as the general key-building path for
    /// every ASCII input pair across the collation flag matrix.
    #[test]
    fn ascii_compare_matches_unicode_compare_for_all_collations() {
        let pairs = [
            ("a", "a"),
            ("a", "A"),
            ("A", "a"),
            ("abc", "abd"),
            ("abc", "abcd"),
            ("abcd", "abc"),
            ("Hello", "hello"),
            ("Hello", "World"),
            ("abc", ""),
            ("", "abc"),
            ("", ""),
            ("01666544319", "15292904661"),
            ("  abc", "abc"), // leading-space differs even under PAD
            ("abc  ", "abc"), // trailing-space depends on pad_attribute
            ("AaA", "aaa"),
            ("Z", "a"), // ASCII case-fold ordering correctness
            ("a", "Z"),
            ("a\0", "a"),
            ("a\0b", "ab"),
            ("\0abc", "abc"),
        ];
        for info in collations_to_check() {
            for (l, r) in pairs {
                let l_trim = trim_for_pad(info, l);
                let r_trim = trim_for_pad(info, r);
                let fast = compare_strings_for_info(info, l, r);
                let general =
                    collation_key_unicode(info, l_trim).cmp(&collation_key_unicode(info, r_trim));
                let general = if info.binary {
                    l_trim.as_bytes().cmp(r_trim.as_bytes())
                } else {
                    general
                };
                assert_eq!(
                    fast, general,
                    "ASCII compare drift for collation {}, pair ({:?}, {:?}): fast={:?} general={:?}",
                    info.name, l, r, fast, general,
                );
            }
        }
    }

    #[test]
    fn ascii_equality_fast_path_matches_comparison_oracle() {
        let pairs = [
            ("MAIL", "mail"),
            ("SHIP", "SHIP"),
            ("RAIL", "MAIL"),
            ("abc  ", "abc"),
            ("a\0b", "ab"),
            ("é", "e"),
            ("Straße", "STRASSE"),
            ("", ""),
        ];
        for info in collations_to_check() {
            for (left, right) in pairs {
                assert_eq!(
                    strings_equal_for_info(info, left, right),
                    compare_strings_for_info(info, left, right) == Ordering::Equal,
                    "equality drift for collation {}, pair ({left:?}, {right:?})",
                    info.name,
                );
            }
        }
    }

    /// The fused single-pass compare must agree with the unicode
    /// collation-key ordering whenever it returns `Some`, across word
    /// boundaries, case folds, and length splits; and it must return
    /// `None` (defer to the key path) whenever a non-ASCII byte sits at
    /// or before the decision point.
    #[test]
    fn ascii_cmp_fused_matches_collation_key_or_defers() {
        let samples = [
            "",
            "a",
            "A",
            "abc",
            "ABC",
            "abcd",
            "abcdefg",
            "abcdefgh",          // exactly one word
            "abcdefghi",         // word + 1
            "ABCDEFGHi",         // case folds across the word boundary
            "abcdefghIJKLMNOPq", // two words + tail
            "abcdefgHijklmnoPQRSTUVWXyz0123456789",
            "01666544319-aaaa",
            "01666544319-aaab",
            "zzzzzzzzzzzzzzzz",
            "éclair",      // non-ASCII head
            "abcdefghé",   // non-ASCII after one word
            "abcé",        // non-ASCII mid
            "abce\u{301}", // combining mark tail
            "abc ",
            " abc",
        ];
        for info in collations_to_check() {
            if info.binary {
                continue;
            }
            for l in samples {
                for r in samples {
                    let l_trim = trim_for_pad(info, l);
                    let r_trim = trim_for_pad(info, r);
                    let general = collation_key_unicode(info, l_trim)
                        .cmp(&collation_key_unicode(info, r_trim));
                    match ascii_cmp_fused(
                        l_trim.as_bytes(),
                        r_trim.as_bytes(),
                        info.case_sensitive,
                        is_uca_0900_collation(info),
                    ) {
                        Some(fused) => assert_eq!(
                            fused, general,
                            "fused compare drift for collation {}, pair ({:?}, {:?})",
                            info.name, l, r,
                        ),
                        None => assert!(
                            !l_trim.is_ascii() || !r_trim.is_ascii(),
                            "fused compare deferred on all-ASCII pair ({:?}, {:?})",
                            l,
                            r,
                        ),
                    }
                }
            }
        }
    }

    /// Sanity for the public surface: PAD SPACE collations still ignore
    /// trailing spaces, accent-insensitive collations still fold é → e,
    /// case-insensitive collations still fold A ↔ a — all on the same
    /// inputs the ASCII path now handles.
    #[test]
    fn public_compare_strings_preserves_mysql_semantics_post_fast_path() {
        // ASCII case insensitive
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "abc", "ABC"),
            Ordering::Equal
        );
        // ASCII ordering
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "abc", "abd"),
            Ordering::Less
        );
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "abc", "abcd"),
            Ordering::Less
        );
        // Trailing-space behaviour driven by pad_attribute (NO PAD)
        assert_ne!(
            compare_strings(DEFAULT_COLLATION, "abc", "abc "),
            Ordering::Equal
        );
        // PAD SPACE collation still trims trailing spaces
        assert_eq!(
            compare_strings("utf8mb4_general_ci", "abc", "abc   "),
            Ordering::Equal
        );
        // Non-ASCII still routed through Unicode path
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "ß", "ss"),
            Ordering::Equal
        );
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "é", "e"),
            Ordering::Equal
        );
        // Mixed ASCII + non-ASCII pair takes the Unicode path
        assert_eq!(
            compare_strings(DEFAULT_COLLATION, "café", "CAFE"),
            Ordering::Equal
        );
    }

    /// `with_collation_key_by_id` must hand the callback the same
    /// bytes that the allocating `collation_key_by_id` would have
    /// returned, for every collation in the flag matrix and every
    /// representative input. Drift here would silently corrupt every
    /// path that switched to the zero-alloc form — hash digests,
    /// B-tree memcmp encoding, statistics digesting — so the property
    /// is checked end-to-end.
    #[test]
    fn with_collation_key_matches_allocating_form() {
        let inputs = [
            "",
            "a",
            "Hello, World!",
            "01666544319-15292904661-62336853652",
            " trailing-space   ",
            "café résumé",
            "ß and ss",
            "中文",
            "日本語",
            "0123456789",
        ];
        for info in collations_to_check() {
            for s in inputs {
                let expected = collation_key_by_id(info.id, s);
                let actual = with_collation_key_by_id(info.id, s, |bytes| bytes.to_vec());
                assert_eq!(
                    expected, actual,
                    "with_collation_key drift for collation {} input {:?}",
                    info.name, s
                );
            }
        }
    }

    #[test]
    fn streamed_collation_prefix_matches_canonical_key_for_all_collations() {
        let inputs = [
            "",
            "ABC xyz",
            "café résumé",
            "Йошкар-Ола",
            "карелки",
            "a\u{301}\u{327}bc",
            "ß Æ œ þ ð ł ø",
            "中文日本語",
            "nul\0ignored",
            "trailing   ",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789--尾部",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789\u{301}",
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345\0lowercase suffix",
        ];
        for info in collations_to_check() {
            for value in inputs {
                let full = collation_key_by_id(info.id, value);
                for limit in [0, 1, 2, 3, 4, 7, 8, 16, 31, 32, 33, 62, 63, 64, 65, 128] {
                    let mut actual = Vec::new();
                    collation_key_prefix_by_id_into(info.id, value, limit, &mut actual);
                    assert_eq!(
                        actual,
                        full[..full.len().min(limit)],
                        "prefix drift for collation {} input {value:?} limit {limit}",
                        info.name,
                    );
                }
            }
        }
    }

    /// `default_string_key_into` writes exactly the bytes that
    /// `default_string_key` would have returned, regardless of the
    /// buffer's prior contents.
    #[test]
    fn default_string_key_into_writes_canonical_bytes() {
        let inputs = ["", "abc", "ABC   ", "café", "中文 abc"];
        for s in inputs {
            let expected = default_string_key(s);
            let mut buf = vec![0xAA, 0xBB, 0xCC];
            default_string_key_into(s, &mut buf);
            assert_eq!(buf, expected, "default_string_key_into drift for {:?}", s);
        }
    }

    /// `collation_key_into` (collation by name) must produce bytes
    /// identical to `collation_key`, and must clear pre-existing
    /// buffer contents.
    #[test]
    fn collation_key_into_matches_allocating_form() {
        let inputs = ["", "a", "Hello", "café", "0123456789"];
        let names = ["utf8mb4_0900_ai_ci", "utf8mb4_bin", "ascii_general_ci"];
        for name in names {
            for s in inputs {
                let expected = collation_key(name, s);
                let mut buf = vec![0xFF; 8];
                collation_key_into(name, s, &mut buf);
                assert_eq!(buf, expected, "collation_key_into drift {name} {s:?}");
            }
        }
    }

    /// `collation_key_by_id_into` mirrors `collation_key_by_id`.
    #[test]
    fn collation_key_by_id_into_matches_allocating_form() {
        let inputs = ["", "abc", "ABC   ", "café"];
        for info in collations_to_check() {
            for s in inputs {
                let expected = collation_key_by_id(info.id, s);
                let mut buf = Vec::new();
                collation_key_by_id_into(info.id, s, &mut buf);
                assert_eq!(
                    buf, expected,
                    "collation_key_by_id_into drift collation {} input {:?}",
                    info.name, s
                );
                buf.fill(0xAA);
                collation_key_for_info_into(info, s, &mut buf);
                assert_eq!(
                    buf, expected,
                    "collation_key_for_info_into drift collation {} input {:?}",
                    info.name, s
                );
            }
        }
    }

    /// The dense-array fast path in `resolve_collation_id` must agree
    /// with the HashMap lookup for every defined collation id, and
    /// must return `None` for an unallocated id.
    #[test]
    fn resolve_collation_id_array_matches_hashmap() {
        for collation in collations() {
            let resolved = resolve_collation_id(collation.id).expect("known id resolves");
            assert!(std::ptr::eq(resolved, collation));
        }
        // 17 is an unallocated MySQL collation slot (was filipino_general_ci, removed).
        // The exact id doesn't matter as long as it's < COLLATION_ID_DIRECT_LIMIT
        // and not present in `collations()`.
        let used_ids: std::collections::HashSet<u16> = collations().iter().map(|c| c.id).collect();
        for candidate in 0u16..COLLATION_ID_DIRECT_LIMIT as u16 {
            if !used_ids.contains(&candidate) {
                assert!(
                    resolve_collation_id(candidate).is_none(),
                    "expected None for unallocated id {}",
                    candidate
                );
                break;
            }
        }
        assert!(resolve_collation_id(u16::MAX).is_none());
    }

    /// Default-collation info is a stable pointer into the static
    /// table — every call returns the same `&'static CollationInfo`,
    /// so cheap shortcuts that compare-by-pointer are sound.
    #[test]
    fn default_collation_info_is_stable() {
        let a = default_collation_info();
        let b = default_collation_info();
        assert!(std::ptr::eq(a, b));
        assert_eq!(a.name, DEFAULT_COLLATION);
    }

    /// Hashing through `with_collation_key_by_id` must produce the
    /// same `u64` as hashing through the previous allocating form —
    /// otherwise the `KeyPart` hash/eq invariant breaks and B-tree
    /// hash-based dedup silently loses rows.
    #[test]
    fn collation_key_hash_stable_across_paths() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let inputs = ["", "abc", "Hello, World!", "café", "中文"];
        for info in collations_to_check() {
            for s in inputs {
                let mut h1 = DefaultHasher::new();
                let key = collation_key_by_id(info.id, s);
                h1.write(&key);
                let v1 = h1.finish();

                let mut h2 = DefaultHasher::new();
                with_collation_key_by_id(info.id, s, |bytes| h2.write(bytes));
                let v2 = h2.finish();

                assert_eq!(
                    v1, v2,
                    "hash drift for collation {} input {:?}",
                    info.name, s
                );
            }
        }
    }
}

/// Encode file text, preserving UTF-8 bytes for binary and UTF-8 charsets.
pub(crate) fn encode_file_text(
    charset: &str,
    text: &str,
) -> Result<Vec<u8>, crate::types::QueryError> {
    if charset.eq_ignore_ascii_case("binary")
        || charset.eq_ignore_ascii_case("utf8")
        || charset.eq_ignore_ascii_case("utf8mb3")
        || charset.eq_ignore_ascii_case("utf8mb4")
    {
        Ok(text.as_bytes().to_vec())
    } else {
        encode_string(charset, text)
    }
}

#[cfg(test)]
#[path = "charset/encoding_tests.rs"]
mod encoding_tests;
