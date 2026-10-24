use super::super::{current_parser_sql_mode_bits, source::SourcePosition, MYSQL_VERSION_ID};
use crate::sql_lex::{segments, SqlLexMode, SqlSegmentKind};
use sqlparser::{
    dialect::Dialect,
    parser::{Parser, ParserError},
    tokenizer::{Location, Span, Token, TokenWithSpan},
};
use std::ops::Range;

/// The upstream MySQL tokenizer treats # as a comment. Mask extension
/// operators with same-width spaces, tokenize once, and inject operator tokens
/// at the original spans. Ordinary input uses the upstream allocation path.
pub(in crate::parser) fn parse_sql(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<Vec<sqlparser::ast::Statement>, ParserError> {
    if !sql.contains("<->") && !sql.contains("<#>") {
        return parse_ordinary_sql(dialect, sql);
    }
    let operators = operator_ranges(sql)?;
    if operators.is_empty() {
        return parse_ordinary_sql(dialect, sql);
    }
    let mut masked = sql.as_bytes().to_vec();
    for span in &operators {
        masked[span.clone()].fill(b' ');
    }
    let masked = String::from_utf8(masked).expect("ASCII operator masking preserves UTF-8");
    let mut tokens = super::literals::tokenize(dialect, &masked)
        .map_err(|error| ParserError::TokenizerError(error.to_string()))?;
    let mut ranges = operators.into_iter().peekable();
    let mut position = SourcePosition::start();
    let mut consumed = 0usize;
    tokens.retain_mut(|token| {
        let start = position
            .seek(sql, token.span.start.line, token.span.start.column)
            .unwrap_or(sql.len());
        if ranges
            .peek()
            .is_some_and(|operator| operator.start == start)
        {
            let operator = ranges.next().unwrap();
            let loc = token.span.start;
            *token = TokenWithSpan::new(
                Token::CustomBinaryOperator(sql[operator.clone()].to_owned()),
                Span::new(loc, Location::new(loc.line, loc.column + 3)),
            );
            consumed = operator.end;
            true
        } else {
            start >= consumed
        }
    });
    if super::literals::may_contain(sql) {
        super::literals::normalize(&mut tokens);
    }
    super::literals::concatenate(&mut tokens);
    validate_complexity(&tokens)?;
    super::locks::expand_lists(&mut tokens);
    super::parameters::assign(&mut tokens)?;
    Parser::new(dialect)
        .with_recursion_limit(UPSTREAM_RECURSION_LIMIT)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

fn parse_ordinary_sql(
    dialect: &dyn Dialect,
    sql: &str,
) -> Result<Vec<sqlparser::ast::Statement>, ParserError> {
    let bit_literals = super::literals::may_contain(sql);
    let mut tokens = super::literals::tokenize(dialect, sql)
        .map_err(|err| ParserError::TokenizerError(err.to_string()))?;
    if bit_literals {
        super::literals::normalize(&mut tokens);
    }
    super::literals::concatenate(&mut tokens);
    validate_complexity(&tokens)?;
    super::locks::expand_lists(&mut tokens);
    super::parameters::assign(&mut tokens)?;
    Parser::new(dialect)
        .with_recursion_limit(UPSTREAM_RECURSION_LIMIT)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

fn operator_ranges(sql: &str) -> Result<Vec<Range<usize>>, ParserError> {
    let mode = SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
    let mut ranges = Vec::new();
    for segment in segments(sql, mode, MYSQL_VERSION_ID) {
        let segment = segment.map_err(|error| ParserError::TokenizerError(error.to_string()))?;
        if segment.kind != SqlSegmentKind::Code {
            continue;
        }
        let mut index = segment.range.start;
        while index + 2 < segment.range.end {
            let Some(next) = memchr::memchr(b'<', &sql.as_bytes()[index..segment.range.end]) else {
                break;
            };
            index += next;
            if matches!(sql.as_bytes().get(index..index + 3), Some(b"<->" | b"<#>")) {
                ranges.push(index..index + 3);
                index += 3;
            } else {
                index += 1;
            }
        }
    }
    Ok(ranges)
}

/// Only an unquoted two-word operator participates in precedence. A bare
/// identifier named `sounds` remains available as a column name or alias.
pub(in crate::parser) fn is_sounds_like(parser: &Parser) -> bool {
    matches!(&parser.peek_token_ref().token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("SOUNDS"))
        && matches!(&parser.peek_nth_token_ref(1).token, Token::Word(word)
            if word.quote_style.is_none() && word.keyword == sqlparser::keywords::Keyword::LIKE)
}

pub(in crate::parser) const SOUNDS_LIKE_OP: &str = "SOUNDS LIKE";

pub(in crate::parser) fn parse_sounds_like(
    parser: &mut Parser,
    left: &sqlparser::ast::Expr,
    precedence: u8,
) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
    if !is_sounds_like(parser) {
        return None;
    }
    Some((|| {
        let _ = parser.next_token();
        parser.expect_keyword(sqlparser::keywords::Keyword::LIKE)?;
        let right = parser.parse_subexpr(precedence)?;
        Ok(sqlparser::ast::Expr::BinaryOp {
            left: Box::new(left.clone()),
            op: sqlparser::ast::BinaryOperator::Custom(SOUNDS_LIKE_OP.into()),
            right: Box::new(right),
        })
    })())
}

/// MySQL high-prefix operators bind above XOR/concatenation. Parse their
/// operands at the correct precedence instead of repairing a converted tree.
pub(in crate::parser) fn parse_high_prefix(
    parser: &mut Parser,
    precedence: u8,
) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
    use sqlparser::ast::{Expr, UnaryOperator};
    let operators: &[UnaryOperator] = match &parser.peek_token_ref().token {
        Token::Plus => &[UnaryOperator::Plus],
        Token::Minus => &[UnaryOperator::Minus],
        // Retain the existing paired-bang contract without rewriting text.
        Token::ExclamationMark | Token::DoubleExclamationMark => &[UnaryOperator::BangNot],
        Token::Tilde => &[UnaryOperator::BitwiseNot],
        // The upstream tokenizer combines PostgreSQL regex/LIKE symbols even
        // under the MySQL dialect. In prefix position these are unary chains.
        Token::ExclamationMarkTilde => &[UnaryOperator::BangNot, UnaryOperator::BitwiseNot],
        Token::DoubleTilde => &[UnaryOperator::BitwiseNot, UnaryOperator::BitwiseNot],
        Token::ExclamationMarkDoubleTilde => &[
            UnaryOperator::BangNot,
            UnaryOperator::BitwiseNot,
            UnaryOperator::BitwiseNot,
        ],
        _ => return None,
    };
    let _ = parser.next_token();
    Some(parser.parse_subexpr(precedence).map(|mut expr| {
        for &op in operators.iter().rev() {
            expr = Expr::UnaryOp {
                op,
                expr: Box::new(expr),
            };
        }
        expr
    }))
}

/// Upstream recursion is one level per nested expression, query or
/// parenthesis; each prefix frame grows its stack on demand (see
/// `MydbDialect::parse_prefix`), so this only bounds total nesting work.
const UPSTREAM_RECURSION_LIMIT: usize = 2 * crate::engine::expr_program::MAX_EXPRESSION_DEPTH
    + 4 * crate::parser::budget::MAX_SELECT_NESTING;

/// Maximum number of set-operation branches in one statement. A chain is a
/// left-deep tree that every planning/execution pass walks recursively, so
/// the bound scales with the statement worker stack: 1024 with the default
/// 8 MiB stack in optimized builds, never below the former 128.
pub(crate) fn max_set_operation_branches() -> usize {
    // Measured worst-case stack per branch, with headroom; unoptimized
    // frames are several times larger.
    const BRANCH_STACK_BYTES: usize = if cfg!(debug_assertions) {
        64 * 1024
    } else {
        8 * 1024
    };
    static LIMIT: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        (crate::util::blocking_pool::worker_stack_size() / BRANCH_STACK_BYTES).clamp(128, 1024)
    })
}

/// Reject unsafe flat trees before sqlparser constructs an AST whose Drop
/// would itself recurse. Quoted tokens and comments never consume this budget.
/// AND/OR chains are parsed balanced (see `parse_logical_chain`) and therefore
/// only end the current operator chain instead of extending it.
fn validate_complexity(tokens: &[TokenWithSpan]) -> Result<(), ParserError> {
    use sqlparser::keywords::Keyword;
    const MAX_CHAIN: usize = crate::engine::expr_program::MAX_EXPRESSION_DEPTH;
    if tokens.len() <= 256 {
        return Ok(());
    }
    #[derive(Default)]
    struct Scope {
        chain: usize,
        child: usize,
        previous: usize,
        // CASE ... END nests like parentheses: its operands are separate
        // subtrees, while operators after END continue the enclosing chain.
        case: bool,
    }
    impl Scope {
        fn depth(&self) -> usize {
            self.previous.max(self.chain + self.child)
        }
        fn separate(&mut self) {
            self.previous = self.depth();
            self.chain = 0;
            self.child = 0;
        }
    }
    fn close(operators: &mut Vec<Scope>) {
        if operators.len() > 1 {
            let depth = operators.pop().unwrap().depth();
            let parent = operators.last_mut().unwrap();
            parent.child = parent.child.max(depth);
        }
    }
    let max_setops = max_set_operation_branches();
    let mut operators = vec![Scope::default()];
    let mut setops = 0usize;
    for token in tokens {
        match &token.token {
            Token::LParen => operators.push(Scope::default()),
            Token::RParen => {
                while operators.len() > 1 && operators.last().unwrap().case {
                    close(&mut operators);
                }
                close(&mut operators);
            }
            Token::Word(word) if word.keyword == Keyword::CASE => operators.push(Scope {
                case: true,
                ..Scope::default()
            }),
            Token::Word(word) if word.keyword == Keyword::END && operators.last().unwrap().case => {
                close(&mut operators);
            }
            Token::Word(word)
                if matches!(word.keyword, Keyword::WHEN | Keyword::THEN | Keyword::ELSE) =>
            {
                operators.last_mut().unwrap().separate();
            }
            Token::Comma => operators.last_mut().unwrap().separate(),
            Token::SemiColon => {
                operators.clear();
                operators.push(Scope::default());
                setops = 0;
            }
            Token::Word(word)
                if matches!(
                    word.keyword,
                    Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT
                ) =>
            {
                operators.last_mut().unwrap().separate();
                setops += 1;
                if setops >= max_setops {
                    return Err(ParserError::ParserError(format!(
                        "{}Set operation exceeds {max_setops} branches",
                        crate::parser::budget::STACK_DEPTH
                    )));
                }
            }
            Token::Plus
            | Token::Minus
            | Token::Mul
            | Token::Div
            | Token::Mod
            | Token::Eq
            | Token::Neq
            | Token::Lt
            | Token::Gt
            | Token::LtEq
            | Token::GtEq
            | Token::Ampersand
            | Token::Pipe
            | Token::Caret
            | Token::ShiftLeft
            | Token::ShiftRight
            | Token::StringConcat
            | Token::Spaceship
            | Token::Arrow
            | Token::LongArrow
            | Token::Assignment
            | Token::CustomBinaryOperator(_) => {
                operators.last_mut().unwrap().chain += 1;
            }
            Token::Word(word) if matches!(word.keyword, Keyword::AND | Keyword::OR) => {
                operators.last_mut().unwrap().separate();
            }
            Token::Word(word)
                if matches!(
                    word.keyword,
                    Keyword::XOR
                        | Keyword::DIV
                        | Keyword::MOD
                        | Keyword::LIKE
                        | Keyword::RLIKE
                        | Keyword::REGEXP
                        | Keyword::IS
                        | Keyword::IN
                        | Keyword::BETWEEN
                        | Keyword::NOT
                ) =>
            {
                operators.last_mut().unwrap().chain += 1;
            }
            _ => {}
        }
        if operators.last().unwrap().depth() > MAX_CHAIN {
            return Err(ParserError::ParserError(format!(
                "{}Expression chain depth exceeds {MAX_CHAIN} operators",
                crate::parser::budget::STACK_DEPTH
            )));
        }
    }
    Ok(())
}

/// AND and OR are associative under SQL three-valued logic. Parse a
/// homogeneous chain iteratively into a balanced upstream tree, so neither
/// upstream destruction nor any upstream walker recurses once per operand.
/// Each operand is parsed exactly as upstream parses the right side of the
/// operator; conversion flattens the chain again in source order.
pub(in crate::parser) fn parse_logical_chain(
    parser: &mut Parser,
    left: &sqlparser::ast::Expr,
    precedence: u8,
) -> Option<Result<sqlparser::ast::Expr, ParserError>> {
    use sqlparser::ast::BinaryOperator;
    fn logical_operator(parser: &Parser) -> Option<BinaryOperator> {
        match &parser.peek_token_ref().token {
            Token::Word(word) if word.keyword == sqlparser::keywords::Keyword::AND => {
                Some(BinaryOperator::And)
            }
            Token::Word(word) if word.keyword == sqlparser::keywords::Keyword::OR => {
                Some(BinaryOperator::Or)
            }
            _ => None,
        }
    }
    let op = logical_operator(parser)?;
    Some((|| {
        let mut operands = vec![left.clone()];
        while logical_operator(parser).as_ref() == Some(&op) {
            let _ = parser.next_token();
            operands.push(parser.parse_subexpr(precedence)?);
        }
        while operands.len() > 1 {
            let mut level = Vec::with_capacity(operands.len().div_ceil(2));
            let mut iter = operands.into_iter();
            while let Some(left) = iter.next() {
                level.push(match iter.next() {
                    Some(right) => sqlparser::ast::Expr::BinaryOp {
                        left: Box::new(left),
                        op: op.clone(),
                        right: Box::new(right),
                    },
                    None => left,
                });
            }
            operands = level;
        }
        Ok(operands.pop().expect("logical chain has operands"))
    })())
}
