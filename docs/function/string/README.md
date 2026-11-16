# String Functions

| Function | Description |
| --- | --- |
| [`ASCII`](ascii.md) | Returns the ASCII code of the first character of a string. |
| [`BIT_LENGTH`](bit_length.md) | Returns the length of a string in bits. |
| [`CHAR`](char.md) | Converts a sequence of integer code values into a string. |
| [`CHARACTER_LENGTH`](character_length.md) | Returns the number of characters in a string; an alias of `CHAR_LENGTH`. |
| [`CHAR_LENGTH`](char_length.md) | Returns the number of characters in a string. |
| [`CONCAT`](concat.md) | Concatenates all arguments in order. |
| [`CONCAT_WS`](concat_ws.md) | Concatenates the remaining non-`NULL` arguments using the first argument as the separator. |
| [`EXPORT_SET`](export_set.md) | Selects the on/off strings according to an integer bitmap and concatenates the result. |
| [`FIND_IN_SET`](find_in_set.md) | Returns the position of a string within a comma-separated set. |
| [`FORMAT`](format.md) | Formats a number with a given number of decimals and an optional locale. |
| [`HEX`](hex.md) | Returns a hexadecimal representation of a string's bytes or an integer. |
| [`INSERT`](insert.md) | Replaces a portion of a string starting at a specified position. |
| [`INSTR`](instr.md) | Returns the position of the first occurrence of a substring within a string. |
| [`LCASE`](lcase.md) | Converts a string to lowercase; an alias of `LOWER`. |
| [`LEFT`](left.md) | Returns the leftmost given number of characters of a string. |
| [`LENGTH`](length.md) | Returns the length of a string in bytes. |
| [`LIST_ADD`](list_add.md) | Appends an element to a comma-separated list if it is not already present. |
| [`LIST_DROP`](list_drop.md) | Removes a given element from a comma-separated list. |
| [`LOCATE`](locate.md) | Returns the position of a substring within a string, optionally from a given starting position. |
| [`LOWER`](lower.md) | Converts a string to lowercase. |
| [`LPAD`](lpad.md) | Left-pads a string to a specified length. |
| [`LTRIM`](ltrim.md) | Removes leading spaces from a string. |
| [`MAKE_SET`](make_set.md) | Selects arguments according to a bitmap and joins them into a comma-separated string. |
| [`MID`](mid.md) | Extracts a substring; an alias of `SUBSTRING`. |
| [`OCTET_LENGTH`](octet_length.md) | Returns the length of a string in bytes; an alias of `LENGTH`. |
| [`ORD`](ord.md) | Returns the multi-byte encoded value of the first character of a string. |
| [`POSITION`](position.md) | Returns the position of the first occurrence of a substring within a string. |
| [`QUOTE`](quote.md) | Escapes a string into single-quoted text suitable for embedding in SQL. |
| [`QUOTE_IDENTIFIER`](quote_identifier.md) | Quotes and escapes an SQL identifier. |
| [`REGEXP_INSTR`](regexp_instr.md) | Returns the position of a regular expression match. |
| [`REGEXP_LIKE`](regexp_like.md) | Determines whether a string matches a regular expression. |
| [`REGEXP_REPLACE`](regexp_replace.md) | Replaces regular expression matches with replacement text. |
| [`REGEXP_SUBSTR`](regexp_substr.md) | Returns the substring matched by a regular expression. |
| [`REPEAT`](repeat.md) | Repeats a string a given number of times. |
| [`REPLACE`](replace.md) | Replaces all occurrences of a given substring within a string with new content. |
| [`REVERSE`](reverse.md) | Reverses a string. |
| [`RIGHT`](right.md) | Returns the rightmost given number of characters of a string. |
| [`RPAD`](rpad.md) | Right-pads a string to a specified length. |
| [`RTRIM`](rtrim.md) | Removes trailing spaces from a string. |
| [`SOUNDEX`](soundex.md) | Returns the Soundex phonetic code of a string. |
| [`SPACE`](space.md) | Returns a string of a given number of spaces. |
| [`STRCMP`](strcmp.md) | Compares two strings and returns -1, 0, or 1. |
| [`SUBSTR`](substr.md) | Extracts a substring; an alias of `SUBSTRING`. |
| [`SUBSTRING`](substring.md) | Extracts a substring from a specified position, optionally with a given length. |
| [`SUBSTRING_INDEX`](substring_index.md) | Returns one side of a string split by a delimiter occurrence count. |
| [`TRIM`](trim.md) | Removes spaces or a given character sequence from both ends or a specified direction of a string. |
| [`UCASE`](ucase.md) | Converts a string to uppercase; an alias of `UPPER`. |
| [`UNHEX`](unhex.md) | Decodes hexadecimal text into a byte string. |
| [`UPPER`](upper.md) | Converts a string to uppercase. |


## Text and binary semantics

Text strings have a character set and collation; binary strings are byte sequences.
LENGTH/OCTET_LENGTH and BIT_LENGTH measure encoded bytes, whereas CHAR_LENGTH and
text slicing count Unicode scalar characters, not display width or grapheme clusters.
ASCII reads the first encoded byte and ORD combines the encoded bytes of the first
character. Use `SET NAMES utf8mb4` for the multibyte examples on these pages.

Search/position and STRCMP functions use their resolved comparison semantics.
REPLACE and SUBSTRING_INDEX delimiter matching are case-sensitive. LOWER/UPPER
perform character conversion; binary values do not have letter case. Do not assume
that a case-insensitive connection makes every string operation case-insensitive.
Positions start at 1. SUBSTRING supports negative positions; a position returned
by INSTR/LOCATE is 0 when no match exists.

NULL is distinct from empty text. CONCAT propagates NULL; CONCAT_WS skips NULL
values but returns NULL for a NULL separator. CHAR skips NULL code arguments;
LIST_ADD/LIST_DROP reject NULL; QUOTE(NULL) returns the text NULL. The function
pages state these exceptions explicitly.

REPEAT/SPACE/LPAD/RPAD and other constructed strings are bounded by
max_allowed_packet and governed memory. A packet-overflow result can be NULL with
warning 1301; memory/cancellation failures are errors. Use HEX for binary output
and prepared placeholders for application values, rather than building SQL literals.


## Regular expression rules

| Argument | Default and meaning |
| --- | --- |
| pos | 1; one-based start position |
| occurrence in INSTR/SUBSTR | 1; positive match number |
| occurrence in REPLACE | 0; replace every match, or a positive selected occurrence |
| return_option in INSTR | 0 for match start, 1 for the position just after its end |
| match_type | Resolved collation supplies the default case sensitivity |

| Flag | Effect |
| --- | --- |
| i | Case-insensitive matching |
| c | Case-sensitive matching |
| m | Multiline anchors |
| n | Dot matches line terminators |
| u | Unix line-ending behavior |

When c and i both occur, the rightmost setting wins. Unknown flags are errors.
Patterns support ordinary alternation, groups, classes, repetition, anchors and
the implemented MySQL/POSIX constructs. Unsupported constructs, including pattern
backreferences, raise a regex diagnostic rather than being silently ignored.
ICU_VERSION is a compatibility string, not evidence of a complete ICU engine.

Text positions count characters. All-binary operands use byte matching and preserve
binary results; mixing binary/text regex operands raises 3995. Empty matches are
valid; replacement advances after them rather than repeatedly replacing at one
position. INSTR returns 0 for no match; SUBSTR returns NULL; REPLACE preserves the
source. Patterns exceeding 8192 encoded bytes raise the implementation resource-limit diagnostic.
Numeric bounds and pattern/result memory are checked before unbounded work.

SQL string parsing consumes backslashes before the regex parser sees them. Under
the ordinary SQL mode, double a backslash that must reach a pattern or replacement;
NO_BACKSLASH_ESCAPES changes that SQL-literal step. Replacement capture references
use $1, $2 and the supported ${name} form; backslash escapes a following replacement
character and is not the capture-number introducer; use prepared parameters to avoid
confusing SQL escaping with regex escaping.
