use super::*;

#[test]
fn explain_binary_tokens_keep_spacing_and_operand_parentheses() {
    use BinaryOp::*;
    for (op, expected) in [
        (Or, "a OR b"),
        (And, "a AND b"),
        (Xor, "a XOR b"),
        (Eq, "a=b"),
        (NullSafeEq, "a<=>b"),
        (NotEq, "a<>b"),
        (Lt, "a<b"),
        (LtEq, "a<=b"),
        (Gt, "a>b"),
        (GtEq, "a>=b"),
        (Add, "a+b"),
        (Sub, "a-b"),
        (Mul, "a*b"),
        (Div, "a/b"),
        (IntDiv, "aDIVb"),
        (Mod, "a%b"),
        (BitAnd, "a&b"),
        (BitOr, "a|b"),
        (BitXor, "a^b"),
        (ShiftLeft, "a<<b"),
        (ShiftRight, "a>>b"),
        (Arrow, "a->b"),
        (LongArrow, "a->>b"),
        (Concat, "a||b"),
    ] {
        let expr = Expr::Binary {
            left: Box::new(Expr::Identifier("a".into())),
            op,
            right: Box::new(Expr::Identifier("b".into())),
        };
        assert_eq!(expr_to_explain_string(&expr), expected);
    }
    let expr = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("a".into())),
            op: Add,
            right: Box::new(Expr::Identifier("b".into())),
        }),
        op: Mul,
        right: Box::new(Expr::Identifier("c".into())),
    };
    assert_eq!(expr_to_explain_string(&expr), "a+b*c");
    let logical = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Identifier("a".into())),
            op: Or,
            right: Box::new(Expr::Identifier("b".into())),
        }),
        op: And,
        right: Box::new(Expr::Identifier("c".into())),
    };
    assert_eq!(expr_to_explain_string(&logical), "(a OR b) AND c");
}
