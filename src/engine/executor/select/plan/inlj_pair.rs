//! Shared candidate-loop control flow. This macro deliberately expands the
//! original continue/break/? operations in the caller's loop: returning a
//! separate control value altered this hot operator's stack/code generation.
//! All inputs are explicit; matched is sticky, SEMI breaks, ANTI continues.
macro_rules! emit_inlj_pair {
    ($db:ident, $txn:ident, $join_type:ident, $semi_or_anti:ident,
     $residual:expr, $residual_pre:ident, $join_lk:ident,
     $left_row:ident, $right_row:ident, $out:ident, $matched:ident, $eval_ctx:ident) => {
        if let Some(residual) = $residual {
            match $residual_pre.get(residual, $left_row, &$right_row) {
                Some(pre) => {
                    if !pre.eval_pair($left_row, &$right_row)? {
                        continue;
                    }
                }
                None => {
                    let joined = $crate::engine::executor::select::plan::join_rows_with_owned_right(
                        $left_row, $right_row, &$join_lk,
                    );
                    if $crate::engine::executor::select::eval::eval_plan_bool_with_db(
                        $db,
                        $txn,
                        residual,
                        &joined,
                        &mut $eval_ctx,
                    )? {
                        $matched = true;
                        if !$semi_or_anti {
                            $out.push(joined);
                        } else if $join_type == $crate::parser::JoinType::Semi {
                            $out.push($left_row.clone());
                            break;
                        }
                    }
                    continue;
                }
            }
        }
        $matched = true;
        if !$semi_or_anti {
            $out.push(
                $crate::engine::executor::select::plan::join_rows_with_owned_right(
                    $left_row, $right_row, &$join_lk,
                ),
            );
        } else if $join_type == $crate::parser::JoinType::Semi {
            $out.push($left_row.clone());
            break;
        }
    };
}

pub(super) use emit_inlj_pair;
