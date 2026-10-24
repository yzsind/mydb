//! Identities of SQL constructs currently lowered to scalar calls. These are
//! syntax operations, so a cost-model miss must not turn CAST into a UDF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntrinsicFunction {
    Cast,
    Collate,
    ConvertUsing,
    Extract,
    Interval,
    IntervalList,
    AtTimeZone,
    TimestampLiteral,
    IsTrue,
    IsFalse,
    LikeEscape,
    MemberOf,
    MatchAgainst,
    OrderedGroupConcat,
    HiddenFirstValue,
    UserAssignment,
    EnumOrdinal,
    SetOrdinal,
    EnumComparison,
    SetComparison,
    FindInSet,
    CollationSortKey,
    CollatedValue,
}

impl IntrinsicFunction {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        const NAMES: &[(&str, IntrinsicFunction)] = &[
            ("__ENUM_ORDINAL", IntrinsicFunction::EnumOrdinal),
            ("__SET_ORDINAL", IntrinsicFunction::SetOrdinal),
            ("__SET_ORDINAL_BINARY", IntrinsicFunction::SetOrdinal),
            ("__ENUM_CMP", IntrinsicFunction::EnumComparison),
            ("__SET_CMP", IntrinsicFunction::SetComparison),
            ("__SET_CMP_BINARY", IntrinsicFunction::SetComparison),
            ("__FIND_IN_SET_SET", IntrinsicFunction::FindInSet),
            ("__FIND_IN_SET_SET_BINARY", IntrinsicFunction::FindInSet),
            ("__COLLATION_SORT_KEY", IntrinsicFunction::CollationSortKey),
            ("__COLLATED_CONST", IntrinsicFunction::CollatedValue),
            ("CAST", IntrinsicFunction::Cast),
            ("COLLATE", IntrinsicFunction::Collate),
            ("CONVERT_USING", IntrinsicFunction::ConvertUsing),
            ("EXTRACT", IntrinsicFunction::Extract),
            ("__INTERVAL", IntrinsicFunction::Interval),
            ("__INTERVAL_LIST", IntrinsicFunction::IntervalList),
            ("__AT_TIME_ZONE", IntrinsicFunction::AtTimeZone),
            ("__TIMESTAMP_LITERAL", IntrinsicFunction::TimestampLiteral),
            ("__IS_TRUE", IntrinsicFunction::IsTrue),
            ("__IS_FALSE", IntrinsicFunction::IsFalse),
            ("__LIKE_DYNAMIC_ESCAPE", IntrinsicFunction::LikeEscape),
            ("__MEMBER_OF", IntrinsicFunction::MemberOf),
            (super::FT_MATCH_FUNCTION, IntrinsicFunction::MatchAgainst),
            (
                super::GROUP_CONCAT_ORDERED_FN,
                IntrinsicFunction::OrderedGroupConcat,
            ),
            (
                super::HIDDEN_FIRST_VALUE_FN,
                IntrinsicFunction::HiddenFirstValue,
            ),
            (
                super::USER_VAR_ASSIGN_FUNCTION,
                IntrinsicFunction::UserAssignment,
            ),
        ];
        NAMES
            .iter()
            .find(|(candidate, _)| name.eq_ignore_ascii_case(candidate))
            .map(|(_, kind)| *kind)
    }
}
