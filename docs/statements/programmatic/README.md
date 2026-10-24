# Stored Programs, Triggers, and Events

| Group | Statements |
| --- | --- |
| Procedure | [CREATE](create-procedure.md) · [ALTER](alter-procedure.md) · [DROP](drop-procedure.md) · [CALL](call.md) |
| Function | [CREATE](create-function.md) · [ALTER](alter-function.md) · [DROP](drop-function.md) |
| Trigger | [CREATE](create-trigger.md) · [DROP](drop-trigger.md) |
| Event | [CREATE](create-event.md) · [ALTER](alter-event.md) · [DROP](drop-event.md) |
| Dynamic SQL | [PREPARE](prepare.md) · [EXECUTE](execute.md) · [DEALLOCATE](deallocate-prepare.md) |
| Session SQL cursors | [DECLARE](declare-cursor.md) · [FETCH](fetch-cursor.md) · [CLOSE](close-cursor.md) |
| Blocks and declarations | [BEGIN…END](begin-end.md) · [Variables](declare-variable.md) · [CONDITION](declare-condition.md) · [CURSOR](declare-routine-cursor.md) · [HANDLER](declare-handler.md) |
| Assignment and query | [SET](routine-set.md) · [SELECT INTO](select-into.md) |
| Control flow | [IF](if.md) · [CASE](case.md) · [LOOP](loop.md) · [WHILE](while.md) · [REPEAT](repeat.md) · [LEAVE](leave.md) · [ITERATE](iterate.md) · [RETURN](return.md) |
| Routine cursors | [OPEN](open.md) · [FETCH](fetch.md) · [CLOSE](close.md) |
| Conditions and diagnostics | [SIGNAL](signal.md) · [RESIGNAL](resignal.md) · [GET DIAGNOSTICS](get-diagnostics.md) |
| Client compatibility | [DELIMITER](delimiter.md) |

Routine bodies may also execute ordinary SELECT, DML, CALL, and permitted transaction statements; these statements follow the privilege, transaction, and error semantics documented in their own pages.
