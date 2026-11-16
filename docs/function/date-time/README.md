# Date and Time Functions

| Function | Description |
| --- | --- |
| [`ADDDATE`](adddate.md) | Adds an interval to a date or datetime; an alias of `DATE_ADD`. |
| [`ADDTIME`](addtime.md) | Adds a time value to a time or datetime. |
| [`CONVERT_TZ`](convert_tz.md) | Converts a datetime between two time zones. |
| [`CURDATE`](curdate.md) | Returns the current date in the session time zone. |
| [`CURRENT_DATE`](current_date.md) | Returns the current date in the session time zone; an alias of `CURDATE`. |
| [`CURRENT_TIME`](current_time.md) | Returns the current time in the session time zone; an alias of `CURTIME`. |
| [`CURRENT_TIMESTAMP`](current_timestamp.md) | Returns the current datetime in the session time zone; an alias of `NOW`. |
| [`CURTIME`](curtime.md) | Returns the current time in the session time zone. |
| [`DATE`](date.md) | Extracts the date part of a date or datetime. |
| [`DATEDIFF`](datediff.md) | Returns the number of days between two dates. |
| [`DATE_ADD`](date_add.md) | Adds an interval to a date or datetime. |
| [`DATE_FORMAT`](date_format.md) | Formats a datetime according to a format string. |
| [`DATE_SUB`](date_sub.md) | Subtracts an interval from a date or datetime. |
| [`DAY`](day.md) | Returns the day of the month for a date; an alias of `DAYOFMONTH`. |
| [`DAYNAME`](dayname.md) | Returns the localized weekday name for a date. |
| [`DAYOFMONTH`](dayofmonth.md) | Returns the day of the month for a date. |
| [`DAYOFWEEK`](dayofweek.md) | Returns the weekday index for a date, with Sunday as 1. |
| [`DAYOFYEAR`](dayofyear.md) | Returns the day of the year for a date. |
| [`EXTRACT`](extract.md) | Extracts a datetime field in the specified unit. |
| [`FROM_DAYS`](from_days.md) | Converts a day number to a date. |
| [`FROM_UNIXTIME`](from_unixtime.md) | Converts a Unix timestamp to a datetime in the session time zone, with optional formatting. |
| [`GET_FORMAT`](get_format.md) | Returns a date or time format string for the specified locale. |
| [`HOUR`](hour.md) | Returns the hour part of a time value. |
| [`LAST_DAY`](last_day.md) | Returns the last day of the month for the argument. |
| [`LOCALTIME`](localtime.md) | Returns the current datetime in the session time zone; an alias of `NOW`. |
| [`LOCALTIMESTAMP`](localtimestamp.md) | Returns the current datetime in the session time zone; an alias of `NOW`. |
| [`MAKEDATE`](makedate.md) | Constructs a date from a year and day-of-year number. |
| [`MAKETIME`](maketime.md) | Constructs a time value from hours, minutes, and seconds. |
| [`MICROSECOND`](microsecond.md) | Returns the microsecond part of a time value. |
| [`MINUTE`](minute.md) | Returns the minute part of a time value. |
| [`MONTH`](month.md) | Returns the month of a date. |
| [`MONTHNAME`](monthname.md) | Returns the localized month name for a date. |
| [`NOW`](now.md) | Returns the current datetime in the session time zone, with optional fractional-seconds precision. |
| [`PERIOD_ADD`](period_add.md) | Adds a number of months to a `YYYYMM` or `YYMM` period value. |
| [`PERIOD_DIFF`](period_diff.md) | Returns the number of months between two period values. |
| [`QUARTER`](quarter.md) | Returns the quarter of the year for a date. |
| [`SECOND`](second.md) | Returns the second part of a time value. |
| [`SEC_TO_TIME`](sec_to_time.md) | Converts a number of seconds to a time value. |
| [`STR_TO_DATE`](str_to_date.md) | Parses a datetime string according to a format string. |
| [`SUBDATE`](subdate.md) | Subtracts an interval from a date or datetime; an alias of `DATE_SUB`. |
| [`SUBTIME`](subtime.md) | Subtracts a time value from a time or datetime. |
| [`SYSDATE`](sysdate.md) | Returns the current datetime at the time the function executes. |
| [`TIME`](time.md) | Extracts the time part of a datetime. |
| [`TIMEDIFF`](timediff.md) | Returns the difference between two time or datetime values. |
| [`TIMESTAMP`](timestamp.md) | Combines a date and an optional time into a datetime. |
| [`TIMESTAMPADD`](timestampadd.md) | Adds an integer interval in the specified unit to a datetime. |
| [`TIMESTAMPDIFF`](timestampdiff.md) | Returns the difference between two datetimes in the specified unit. |
| [`TIME_FORMAT`](time_format.md) | Formats a time value according to a format string. |
| [`TIME_TO_SEC`](time_to_sec.md) | Converts a time value to a number of seconds. |
| [`TO_DAYS`](to_days.md) | Converts a date to a day number. |
| [`TO_SECONDS`](to_seconds.md) | Converts a datetime to a seconds number. |
| [`UNIX_TIMESTAMP`](unix_timestamp.md) | Returns the current Unix timestamp, or converts a datetime to a Unix timestamp. |
| [`UTC_DATE`](utc_date.md) | Returns the current UTC date. |
| [`UTC_TIME`](utc_time.md) | Returns the current UTC time. |
| [`UTC_TIMESTAMP`](utc_timestamp.md) | Returns the current UTC datetime. |
| [`WEEK`](week.md) | Returns the week number of a date with an optional mode. |
| [`WEEKDAY`](weekday.md) | Returns the weekday index for a date, with Monday as 0. |
| [`WEEKOFYEAR`](weekofyear.md) | Returns the ISO-style week number of the year for a date. |
| [`YEAR`](year.md) | Returns the year of a date. |
| [`YEARWEEK`](yearweek.md) | Returns an integer composed of the year and week number. |


## Temporal inputs and precision

DATE, DATETIME and TIME values can be supplied as typed columns/literals or matching
text/numeric forms. Functions extract or convert only the domains documented for
their arguments; a numeric count is not itself a datetime. SQL NULL propagation
is stated on each page. Incomplete dates, zero dates, malformed values and overflow
can differ between SELECT warnings and strict data-changing errors; inspect
`SHOW WARNINGS` and the final statement status.

Calendar values use years through 9999. TIME is a signed duration with a maximum
magnitude near 838:59:59, not a clock limited to 23:59:59. Fractional seconds use
up to six digits. Explicit fsp arguments must be 0..6 and default to 0. Fractional
conversion can round, or truncate with TIME_TRUNCATE_FRACTIONAL; a rounding carry
can itself overflow. Month/year arithmetic clamps an unavailable day to month end.

NOW/CURRENT_TIMESTAMP, current date/time and UTC variants use one statement-start
instant; SET timestamp can supply a test instant. SYSDATE uses the evaluation clock
and ignores SET timestamp. Session time_zone affects local current values and
Unix conversions; UTC variants do not use its wall-clock display. lc_time_names
controls localized output names. Day-number conversions use MySQL's proleptic
calendar convention, not Unix epoch days; they are not historical-calendar tools.

CONVERT_TZ accepts fixed offsets and supported IANA zone names without requiring
MySQL timezone tables. Fixed-offset forms and named zones are validated; unknown
zones return NULL. DST gaps/folds follow the zone transition rules, so local time
is not always a unique UTC instant. Use UTC for persisted instants and explicit
zone conversion at presentation boundaries. See its page for pre-epoch and
extended year-9999 bounds.


## Week modes

| Mode | First day | Range | Week 1 rule |
| ---: | --- | --- | --- |
| 0 | Sunday | 0..53 | First Sunday in the year |
| 1 | Monday | 0..53 | At least four days in the year |
| 2 | Sunday | 1..53 | First Sunday in the year |
| 3 | Monday | 1..53 | At least four days in the year |
| 4 | Sunday | 0..53 | At least four days in the year |
| 5 | Monday | 0..53 | First Monday in the year |
| 6 | Sunday | 1..53 | At least four days in the year |
| 7 | Monday | 1..53 | First Monday in the year |

WEEK(date) reads default_week_format. YEARWEEK(date) defaults to mode 0 but returns
the matching week-year, which can be the preceding/following calendar year.
WEEKOFYEAR always uses mode 3. Its accepted second mydb compatibility argument
does not select a different mode; use WEEK for that purpose.
DAYOFWEEK uses Sunday=1; WEEKDAY uses Monday=0.


## Interval units

DATE_ADD/DATE_SUB and EXTRACT support simple YEAR, QUARTER, MONTH, WEEK, DAY,
HOUR, MINUTE, SECOND and MICROSECOND units and the compound units below.
TIMESTAMPADD/TIMESTAMPDIFF use simple units; their count/result is an integer.
An unsupported unit or invalid compound shape is rejected.

| Compound unit | Interval text shape |
| --- | --- |
| YEAR_MONTH | years-months |
| DAY_HOUR | days hours |
| DAY_MINUTE | days hours:minutes |
| DAY_SECOND | days hours:minutes:seconds |
| DAY_MICROSECOND | days hours:minutes:seconds.microseconds |
| HOUR_MINUTE | hours:minutes |
| HOUR_SECOND | hours:minutes:seconds |
| HOUR_MICROSECOND | hours:minutes:seconds.microseconds |
| MINUTE_SECOND | minutes:seconds |
| MINUTE_MICROSECOND | minutes:seconds.microseconds |
| SECOND_MICROSECOND | seconds.microseconds |

Compound EXTRACT results pack the selected fields numerically; YEAR_MONTH yields
YYYYMM and DAY_SECOND yields DDHHMMSS. Negative interval amounts reverse addition
or subtraction. DATE_ADD and TIMESTAMPADD are not fixed-second arithmetic for months.


## Format specifiers

| Specifiers | Meaning |
| --- | --- |
| %Y, %y | Four-digit / two-digit year |
| %m, %c | Month with / without zero padding |
| %d, %e, %D | Day padded / unpadded / with English ordinal suffix |
| %M, %b | Full / abbreviated month name |
| %W, %a, %w | Full / abbreviated weekday name; weekday number Sunday=0 |
| %j | Day of year, 001..366 |
| %H, %k | 24-hour or extended TIME hours, padded / unpadded |
| %h, %I, %l | 12-hour clock hours, padded / padded / unpadded |
| %i | Minute |
| %s, %S | Second |
| %f | Six-digit microsecond field |
| %p | AM or PM |
| %r, %T | 12-hour time with AM/PM; 24-hour HH:MM:SS |
| %U, %u, %V, %v | Week number using the corresponding Sunday/Monday rules |
| %X, %x | Week-year paired with %V / %v |
| %% | Literal percent sign |

DATE_FORMAT renders names using lc_time_names. TIME_FORMAT accepts time fields and renders zero calendar numeric fields for
time-only input; calendar-dependent names/week/day-of-year specifiers return NULL.
STR_TO_DATE uses the format to select a DATE, TIME or DATETIME result and validates
input fields; a valid formatted prefix and a fully consumed input are different
conditions. Do not assume that formatting and parsing are inverses for incomplete
dates, localized names or week-year patterns. A literal unrecognized DATE_FORMAT
specifier renders its character rather than defining a new calendar field.


## Predefined formats

GET_FORMAT accepts DATE, TIME and DATETIME (TIMESTAMP uses DATETIME patterns).
The locale argument is one of the style names below.

| Style | DATE | TIME | DATETIME |
| --- | --- | --- | --- |
| USA | %m.%d.%Y | %h:%i:%s %p | %Y-%m-%d %H.%i.%s |
| JIS | %Y-%m-%d | %H:%i:%s | %Y-%m-%d %H:%i:%s |
| ISO | %Y-%m-%d | %H:%i:%s | %Y-%m-%d %H:%i:%s |
| EUR | %d.%m.%Y | %H.%i.%s | %Y-%m-%d %H.%i.%s |
| INTERNAL | %Y%m%d | %H%i%s | %Y%m%d%H%i%s |

These are format styles, not timezone conversions. ISO here names the predefined
SQL format style; it does not implement every ISO 8601 serialization form.
