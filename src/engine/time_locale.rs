//! `lc_time_names` month/day name tables for DAYNAME() / MONTHNAME() /
//! DATE_FORMAT's `%M %W %a %b` specifiers. MySQL sources these from its
//! built-in locale list; mydb ships the common subset below and falls
//! back to `en_US` for anything it doesn't know (same spirit as the
//! FORMAT() locale table in `expr.rs`).

pub(crate) struct TimeLocale {
    /// Full month names, January..December.
    pub months: [&'static str; 12],
    /// Abbreviated month names.
    pub months_abbr: [&'static str; 12],
    /// Full day names, Monday..Sunday (chrono's weekday ordering via
    /// `num_days_from_monday`).
    pub days: [&'static str; 7],
    /// Abbreviated day names, Monday..Sunday.
    pub days_abbr: [&'static str; 7],
}

const EN_US: TimeLocale = TimeLocale {
    months: [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ],
    months_abbr: [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ],
    days: [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ],
    days_abbr: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
};

const DE_DE: TimeLocale = TimeLocale {
    months: [
        "Januar",
        "Februar",
        "März",
        "April",
        "Mai",
        "Juni",
        "Juli",
        "August",
        "September",
        "Oktober",
        "November",
        "Dezember",
    ],
    months_abbr: [
        "Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez",
    ],
    days: [
        "Montag",
        "Dienstag",
        "Mittwoch",
        "Donnerstag",
        "Freitag",
        "Samstag",
        "Sonntag",
    ],
    days_abbr: ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"],
};

const FR_FR: TimeLocale = TimeLocale {
    months: [
        "janvier",
        "février",
        "mars",
        "avril",
        "mai",
        "juin",
        "juillet",
        "août",
        "septembre",
        "octobre",
        "novembre",
        "décembre",
    ],
    months_abbr: [
        "janv", "févr", "mars", "avr", "mai", "juin", "juil", "août", "sept", "oct", "nov", "déc",
    ],
    days: [
        "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche",
    ],
    days_abbr: ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"],
};

const ES_ES: TimeLocale = TimeLocale {
    months: [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ],
    months_abbr: [
        "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic",
    ],
    days: [
        "lunes",
        "martes",
        "miércoles",
        "jueves",
        "viernes",
        "sábado",
        "domingo",
    ],
    days_abbr: ["lun", "mar", "mié", "jue", "vie", "sáb", "dom"],
};

const IT_IT: TimeLocale = TimeLocale {
    months: [
        "gennaio",
        "febbraio",
        "marzo",
        "aprile",
        "maggio",
        "giugno",
        "luglio",
        "agosto",
        "settembre",
        "ottobre",
        "novembre",
        "dicembre",
    ],
    months_abbr: [
        "gen", "feb", "mar", "apr", "mag", "giu", "lug", "ago", "set", "ott", "nov", "dic",
    ],
    days: [
        "lunedì",
        "martedì",
        "mercoledì",
        "giovedì",
        "venerdì",
        "sabato",
        "domenica",
    ],
    days_abbr: ["lun", "mar", "mer", "gio", "ven", "sab", "dom"],
};

const PT_BR: TimeLocale = TimeLocale {
    months: [
        "janeiro",
        "fevereiro",
        "março",
        "abril",
        "maio",
        "junho",
        "julho",
        "agosto",
        "setembro",
        "outubro",
        "novembro",
        "dezembro",
    ],
    months_abbr: [
        "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez",
    ],
    days: [
        "segunda-feira",
        "terça-feira",
        "quarta-feira",
        "quinta-feira",
        "sexta-feira",
        "sábado",
        "domingo",
    ],
    days_abbr: ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"],
};

const ZH_CN: TimeLocale = TimeLocale {
    months: [
        "一月",
        "二月",
        "三月",
        "四月",
        "五月",
        "六月",
        "七月",
        "八月",
        "九月",
        "十月",
        "十一月",
        "十二月",
    ],
    months_abbr: [
        "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月",
    ],
    days: [
        "星期一",
        "星期二",
        "星期三",
        "星期四",
        "星期五",
        "星期六",
        "星期日",
    ],
    days_abbr: ["周一", "周二", "周三", "周四", "周五", "周六", "周日"],
};

const JA_JP: TimeLocale = TimeLocale {
    months: [
        "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月",
    ],
    months_abbr: [
        "1月", "2月", "3月", "4月", "5月", "6月", "7月", "8月", "9月", "10月", "11月", "12月",
    ],
    days: [
        "月曜日",
        "火曜日",
        "水曜日",
        "木曜日",
        "金曜日",
        "土曜日",
        "日曜日",
    ],
    days_abbr: ["月", "火", "水", "木", "金", "土", "日"],
};

const KO_KR: TimeLocale = TimeLocale {
    months: [
        "1월", "2월", "3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월",
    ],
    months_abbr: [
        "1월", "2월", "3월", "4월", "5월", "6월", "7월", "8월", "9월", "10월", "11월", "12월",
    ],
    days: [
        "월요일",
        "화요일",
        "수요일",
        "목요일",
        "금요일",
        "토요일",
        "일요일",
    ],
    days_abbr: ["월", "화", "수", "목", "금", "토", "일"],
};

/// Resolve an `lc_time_names` value to its name tables. Region variants
/// collapse to the language default we ship (`fr_CA` → `fr_FR` tables);
/// unknown locales fall back to `en_US`.
pub(crate) fn time_locale(name: &str) -> &'static TimeLocale {
    let lang = name.trim().split(['_', '-']).next().unwrap_or("");
    let [first, second] = lang.as_bytes() else {
        return &EN_US;
    };
    match [first.to_ascii_lowercase(), second.to_ascii_lowercase()] {
        [b'd', b'e'] => &DE_DE,
        [b'f', b'r'] => &FR_FR,
        [b'e', b's'] => &ES_ES,
        [b'i', b't'] => &IT_IT,
        [b'p', b't'] => &PT_BR,
        [b'z', b'h'] => &ZH_CN,
        [b'j', b'a'] => &JA_JP,
        [b'k', b'o'] => &KO_KR,
        _ => &EN_US,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn falls_back_to_en_us() {
        assert_eq!(time_locale("xx_XX").months[0], "January");
        assert_eq!(time_locale("").days[6], "Sunday");
    }

    #[test]
    fn resolves_language_variants() {
        assert_eq!(time_locale("fr_CA").months[0], "janvier");
        assert_eq!(time_locale("zh_TW").days[0], "星期一");
        assert_eq!(time_locale("de_AT").months_abbr[2], "Mär");
    }
}
