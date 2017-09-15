/*-------------------------------------------------------------------------
 *
 * execExpr.c
 *	  Expression evaluation infrastructure.
 *
 *	During executor startup, we compile each expression tree (which has
 *	previously been processed by the parser and planner) into an ExprState,
 *	using ExecInitExpr() et al.  This converts the tree into a flat array
 *	of ExprEvalSteps, which may be thought of as instructions in a program.
 *	At runtime, we'll execute steps, starting with the first, until we reach
 *	an EEOP_DONE opcode.
 *
 *	This file contains the "compilation" logic.  It is independent of the
 *	specific execution technology we use (switch statement, computed goto,
 *	JIT compilation, etc).
 *
 *	See src/backend/executor/README for some background, specifically the
 *	"Expression Trees and ExprState nodes", "Expression Initialization",
 *	and "Expression Evaluation" sections.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execExpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execExpr.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"


typedef struct LastAttnumInfo
{
	AttrNumber	last_inner;
	AttrNumber	last_outer;
	AttrNumber	last_scan;
} LastAttnumInfo;

static void ExecReadyExpr(ExprState *state);
static void ExecInitExprRec(Expr *node, PlanState *parent, ExprState *state,
				Datum *resv, bool *resnull);
static void ExprEvalPushStep(ExprState *es, const ExprEvalStep *s);
static void ExecInitFunc(ExprEvalStep *scratch, Expr *node, List *args,
			 Oid funcid, Oid inputcollid, PlanState *parent,
			 ExprState *state);
static void ExecInitExprSlots(ExprState *state, Node *node);
static bool get_last_attnums_walker(Node *node, LastAttnumInfo *info);
static void ExecInitWholeRowVar(ExprEvalStep *scratch, Var *variable,
					PlanState *parent);
static void ExecInitArrayRef(ExprEvalStep *scratch, ArrayRef *aref,
				 PlanState *parent, ExprState *state,
				 Datum *resv, bool *resnull);
static bool isAssignmentIndirectionExpr(Expr *expr);
static void ExecInitCoerceToDomain(ExprEvalStep *scratch, CoerceToDomain *ctest,
					   PlanState *parent, ExprState *state,
					   Datum *resv, bool *resnull);


/*
 * ExecInitExpr: prepare an expression tree for execution
 *
 * This function builds and returns an ExprState implementing the given
 * Expr node tree.  The return ExprState can then be handed to ExecEvalExpr
 * for execution.  Because the Expr tree itself is read-only as far as
 * ExecInitExpr and ExecEvalExpr are concerned, several different executions
 * of the same plan tree can occur concurrently.  (But note that an ExprState
 * does mutate at runtime, so it can't be re-used concurrently.)
 *
 * This must be called in a memory context that will last as long as repeated
 * executions of the expression are needed.  Typically the context will be
 * the same as the per-query context of the associated ExprContext.
 *
 * Any Aggref, WindowFunc, or SubPlan nodes found in the tree are added to
 * the lists of such nodes held by the parent PlanState (or more accurately,
 * the AggrefExprState etc. nodes created for them are added).
 *
 * Note: there is no ExecEndExpr function; we assume that any resource
 * cleanup needed will be handled by just releasing the memory context
 * in which the state tree is built.  Functions that require additional
 * cleanup work can register a shutdown callback in the ExprContext.
 *
 *	'node' is the root of the expression tree to compile.
 *	'parent' is the PlanState node that owns the expression.
 *
 * 'parent' may be NULL if we are preparing an expression that is not
 * associated with a plan tree.  (If so, it can't have aggs or subplans.)
 * Such cases should usually come through ExecPrepareExpr, not directly here.
 *
 * Also, if 'node' is NULL, we just return NULL.  This is convenient for some
 * callers that may or may not have an expression that needs to be compiled.
 * Note that a NULL ExprState pointer *cannot* be handed to ExecEvalExpr,
 * although ExecQual and ExecCheck will accept one (and treat it as "true").
 */
ExprState *
ExecInitExpr(Expr *node, PlanState *parent)
{
	ExprState  *state;
	ExprEvalStep scratch;

	/* Special case: NULL expression produces a NULL ExprState pointer */
	if (node == NULL)
		return NULL;

	/* Initialize ExprState with empty step list */
	state = makeNode(ExprState);
	state->expr = node;

	/* Insert EEOP_*_FETCHSOME steps as needed */
	ExecInitExprSlots(state, (Node *) node);

	/* Compile the expression proper */
	ExecInitExprRec(node, parent, state, &state->resvalue, &state->resnull);

	/* Finally, append a DONE step */
	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecReadyExpr(state);

	return state;
}

/*
 * ExecInitQual: prepare a qual for execution by ExecQual
 *
 * Prepares for the evaluation of a conjunctive boolean expression (qual list
 * with implicit AND semantics) that returns true if none of the
 * subexpressions are false.
 *
 * We must return true if the list is empty.  Since that's a very common case,
 * we optimize it a bit further by translating to a NULL ExprState pointer
 * rather than setting up an ExprState that computes constant TRUE.  (Some
 * especially hot-spot callers of ExecQual detect this and avoid calling
 * ExecQual at all.)
 *
 * If any of the subexpressions yield NULL, then the result of the conjunction
 * is false.  This makes ExecQual primarily useful for evaluating WHERE
 * clauses, since SQL specifies that tuples with null WHERE results do not
 * get selected.
 */
ExprState *
ExecInitQual(List *qual, PlanState *parent)
{
	ExprState  *state;
	ExprEvalStep scratch;
	List	   *adjust_jumps = NIL;
	ListCell   *lc;

	/* short-circuit (here and in ExecQual) for empty restriction list */
	if (qual == NIL)
		return NULL;

	Assert(IsA(qual, List));

	state = makeNode(ExprState);
	state->expr = (Expr *) qual;
	/* mark expression as to be used with ExecQual() */
	state->flags = EEO_FLAG_IS_QUAL;

	/* Insert EEOP_*_FETCHSOME steps as needed */
	ExecInitExprSlots(state, (Node *) qual);

	/*
	 * ExecQual() needs to return false for an expression returning NULL. That
	 * allows us to short-circuit the evaluation the first time a NULL is
	 * encountered.  As qual evaluation is a hot-path this warrants using a
	 * special opcode for qual evaluation that's simpler than BOOL_AND (which
	 * has more complex NULL handling).
	 */
	scratch.opcode = EEOP_QUAL;

	/*
	 * We can use ExprState's resvalue/resnull as target for each qual expr.
	 */
	scratch.resvalue = &state->resvalue;
	scratch.resnull = &state->resnull;

	foreach(lc, qual)
	{
		Expr	   *node = (Expr *) lfirst(lc);

		/* first evaluate expression */
		ExecInitExprRec(node, parent, state, &state->resvalue, &state->resnull);

		/* then emit EEOP_QUAL to detect if it's false (or null) */
		scratch.d.qualexpr.jumpdone = -1;
		ExprEvalPushStep(state, &scratch);
		adjust_jumps = lappend_int(adjust_jumps,
								   state->steps_len - 1);
	}

	/* adjust jump targets */
	foreach(lc, adjust_jumps)
	{
		ExprEvalStep *as = &state->steps[lfirst_int(lc)];

		Assert(as->opcode == EEOP_QUAL);
		Assert(as->d.qualexpr.jumpdone == -1);
		as->d.qualexpr.jumpdone = state->steps_len;
	}

	/*
	 * At the end, we don't need to do anything more.  The last qual expr must
	 * have yielded TRUE, and since its result is stored in the desired output
	 * location, we're done.
	 */
	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecReadyExpr(state);

	return state;
}

/*
 * ExecInitCheck: prepare a check constraint for execution by ExecCheck
 *
 * This is much like ExecInitQual/ExecQual, except that a null result from
 * the conjunction is treated as TRUE.  This behavior is appropriate for
 * evaluating CHECK constraints, since SQL specifies that NULL constraint
 * conditions are not failures.
 *
 * Note that like ExecInitQual, this expects input in implicit-AND format.
 * Users of ExecCheck that have expressions in normal explicit-AND format
 * can just apply ExecInitExpr to produce suitable input for ExecCheck.
 */
ExprState *
ExecInitCheck(List *qual, PlanState *parent)
{
	/* short-circuit (here and in ExecCheck) for empty restriction list */
	if (qual == NIL)
		return NULL;

	Assert(IsA(qual, List));

	/*
	 * Just convert the implicit-AND list to an explicit AND (if there's more
	 * than one entry), and compile normally.  Unlike ExecQual, we can't
	 * short-circuit on NULL results, so the regular AND behavior is needed.
	 */
	return ExecInitExpr(make_ands_explicit(qual), parent);
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List *
ExecInitExprList(List *nodes, PlanState *parent)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, nodes)
	{
		Expr	   *e = lfirst(lc);

		result = lappend(result, ExecInitExpr(e, parent));
	}

	return result;
}

/*
 *		ExecBuildProjectionInfo
 *
 * Build a ProjectionInfo node for evaluating the given tlist in the given
 * econtext, and storing the result into the tuple slot.  (Caller must have
 * ensured that tuple slot has a descriptor matching the tlist!)
 *
 * inputDesc can be NULL, but if it is not, we check to see whether simple
 * Vars in the tlist match the descriptor.  It is important to provide
 * inputDesc for relation-scan plan nodes, as a cross check that the relation
 * hasn't been changed since the plan was made.  At higher levels of a plan,
 * there is no need to recheck.
 *
 * This is implemented by internally building an ExprState that performs the
 * whole projection in one go.
 *
 * Caution: before PG v10, the targetList was a list of ExprStates; now it
 * should be the planner-created targetlist, since we do the compilation here.
 */
ProjectionInfo *
ExecBuildProjectionInfo(List *targetList,
						ExprContext *econtext,
						TupleTableSlot *slot,
						PlanState *parent,
						TupleDesc inputDesc)
{
	ProjectionInfo *projInfo = makeNode(ProjectionInfo);
	ExprState  *state;
	ExprEvalStep scratch;
	ListCell   *lc;

	projInfo->pi_exprContext = econtext;
	/* We embed ExprState into ProjectionInfo instead of doing extra palloc */
	projInfo->pi_state.tag.type = T_ExprState;
	state = &projInfo->pi_state;
	state->expr = (Expr *) targetList;
	state->resultslot = slot;

	/* Insert EEOP_*_FETCHSOME steps as needed */
	ExecInitExprSlots(state, (Node *) targetList);

	/* Now compile each tlist column */
	foreach(lc, targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *variable = NULL;
		AttrNumber	attnum = 0;
		bool		isSafeVar = false;

		/*
		 * If tlist expression is a safe non-system Var, use the fast-path
		 * ASSIGN_*_VAR opcodes.  "Safe" means that we don't need to apply
		 * CheckVarSlotCompatibility() during plan startup.  If a source slot
		 * was provided, we make the equivalent tests here; if a slot was not
		 * provided, we assume that no check is needed because we're dealing
		 * with a non-relation-scan-level expression.
		 */
		if (tle->expr != NULL &&
			IsA(tle->expr, Var) &&
			((Var *) tle->expr)->varattno > 0)
		{
			/* Non-system Var, but how safe is it? */
			variable = (Var *) tle->expr;
			attnum = variable->varattno;

			if (inputDesc == NULL)
				isSafeVar = true;	/* can't check, just assume OK */
			else if (attnum <= inputDesc->natts)
			{
				Form_pg_attribute attr = TupleDescAttr(inputDesc, attnum - 1);

				/*
				 * If user attribute is dropped or has a type mismatch, don't
				 * use ASSIGN_*_VAR.  Instead let the normal expression
				 * machinery handle it (which'll possibly error out).
				 */
				if (!attr->attisdropped && variable->vartype == attr->atttypid)
				{
					isSafeVar = true;
				}
			}
		}

		if (isSafeVar)
		{
			/* Fast-path: just generate an EEOP_ASSIGN_*_VAR step */
			switch (variable->varno)
			{
				case INNER_VAR:
					/* get the tuple from the inner node */
					scratch.opcode = EEOP_ASSIGN_INNER_VAR;
					break;

				case OUTER_VAR:
					/* get the tuple from the outer node */
					scratch.opcode = EEOP_ASSIGN_OUTER_VAR;
					break;

					/* INDEX_VAR is handled by default case */

				default:
					/* get the tuple from the relation being scanned */
					scratch.opcode = EEOP_ASSIGN_SCAN_VAR;
					break;
			}

			scratch.d.assign_var.attnum = attnum - 1;
			scratch.d.assign_var.resultnum = tle->resno - 1;
			ExprEvalPushStep(state, &scratch);
		}
		else
		{
			/*
			 * Otherwise, compile the column expression normally.
			 *
			 * We can't tell the expression to evaluate directly into the
			 * result slot, as the result slot (and the exprstate for that
			 * matter) can change between executions.  We instead evaluate
			 * into the ExprState's resvalue/resnull and then move.
			 */
			ExecInitExprRec(tle->expr, parent, state,
							&state->resvalue, &state->resnull);

			/*
			 * Column might be referenced multiple times in upper nodes, so
			 * force value to R/O - but only if it could be an expanded datum.
			 */
			if (get_typlen(exprType((Node *) tle->expr)) == -1)
				scratch.opcode = EEOP_ASSIGN_TMP_MAKE_RO;
			else
				scratch.opcode = EEOP_ASSIGN_TMP;
			scratch.d.assign_tmp.resultnum = tle->resno - 1;
			ExprEvalPushStep(state, &scratch);
		}
	}

	scratch.opcode = EEOP_DONE;
	ExprEvalPushStep(state, &scratch);

	ExecReadyExpr(state);

	return projInfo;
}

/*
 * ExecPrepareExpr --- initialize for expression execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitExpr in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we run the
 * passed expression tree through expression_planner() to prepare it for
 * execution.  (In ordinary Plan trees the regular planning process will have
 * made the appropriate transformations on expressions, but for standalone
 * expressions this won't have happened.)
 */
ExprState *
ExecPrepareExpr(Expr *node, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	node = expression_planner(node);

	result = ExecInitExpr(node, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * ExecPrepareQual --- initialize for qual execution outside a normal
 * Plan tree context.
 *
 * This differs from ExecInitQual in that we don't assume the caller is
 * already running in the EState's per-query context.  Also, we run the
 * passed expression tree through expression_planner() to prepare it for
 * execution.  (In ordinary Plan trees the regular planning process will have
 * made the appropriate transformations on expressions, but for standalone
 * expressions this won't have happened.)
 */
ExprState *
ExecPrepareQual(List *qual, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	qual = (List *) expression_planner((Expr *) qual);

	result = ExecInitQual(qual, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * ExecPrepareCheck -- initialize check constraint for execution outside a
 * normal Plan tree context.
 *
 * See ExecPrepareExpr() and ExecInitCheck() for details.
 */
ExprState *
ExecPrepareCheck(List *qual, EState *estate)
{
	ExprState  *result;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	qual = (List *) expression_planner((Expr *) qual);

	result = ExecInitCheck(qual, NULL);

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * Call ExecPrepareExpr() on each member of a list of Exprs, and return
 * a list of ExprStates.
 *
 * See ExecPrepareExpr() for details.
 */
List *
ExecPrepareExprList(List *nodes, EState *estate)
{
	List	   *result = NIL;
	MemoryContext oldcontext;
	ListCell   *lc;

	/* Ensure that the list cell nodes are in the right context too */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	foreach(lc, nodes)
	{
		Expr	   *e = (Expr *) lfirst(lc);

		result = lappend(result, ExecPrepareExpr(e, estate));
	}

	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * ExecCheck - evaluate a check constraint
 *
 * For check constraints, a null result is taken as TRUE, ie the constraint
 * passes.
 *
 * The check constraint may have been prepared with ExecInitCheck
 * (possibly via ExecPrepareCheck) if the caller had it in implicit-AND
 * format, but a regular boolean expression prepared with ExecInitExpr or
 * ExecPrepareExpr works too.
 */
bool
ExecCheck(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;

	/* short-circuit (here and in ExecInitCheck) for empty restriction list */
	if (state == NULL)
		return true;

	/* verify that expression was not compiled using ExecInitQual */
	Assert(!(state->flags & EEO_FLAG_IS_QUAL));

	ret = ExecEvalExprSwitchContext(state, econtext, &isnull);

	if (isnull)
		return true;

	return DatumGetBool(ret);
}

/*
 * Prepare a compiled expression for execution.  This has to be called for
 * every ExprState before it can be executed.
 *
 * NB: While this currently only calls ExecReadyInterpretedExpr(),
 * this will likely get extended to further expression evaluation methods.
 * Therefore this should be used instead of directly calling
 * ExecReadyInterpretedExpr().
 */
static void
ExecReadyExpr(ExprState *state)
{
	ExecReadyInterpretedExpr(state);
}

/*
 * Append the steps necessary for the evaluation of node to ExprState->steps,
 * possibly recursing into sub-expressions of node.
 *
 * node - expression to evaluate
 * parent - parent executor node (or NULL if a standalone expression)
 * state - ExprState to whose ->steps to append the necessary operations
 * resv / resnull - where to store the result of the node into
 */
static void
ExecInitExprRec(Expr *node, PlanState *parent, ExprState *state,
				Datum *resv, bool *resnull)
{
	ExprEvalStep scratch;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/* Step's output location is always what the caller gave us */
	Assert(resv != NULL && resnull != NULL);
	scratch.resvalue = resv;
	scratch.resnull = resnull;

	/* cases should be ordered as they are in enum NodeTag */
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *variable = (Var *) node;

				if (variable->varattno == InvalidAttrNumber)
				{
					/* whole-row Var */
					ExecInitWholeRowVar(&scratch, variable, parent);
				}
				else if (variable->varattno <= 0)
				{
					/* system column */
					scratch.d.var.attnum = variable->varattno;
					scratch.d.var.vartype = variable->vartype;
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_SYSVAR;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_SYSVAR;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_SYSVAR;
							break;
					}
				}
				else
				{
					/* regular user column */
					scratch.d.var.attnum = variable->varattno - 1;
					scratch.d.var.vartype = variable->vartype;
					/* select EEOP_*_FIRST opcode to force one-time checks */
					switch (variable->varno)
					{
						case INNER_VAR:
							scratch.opcode = EEOP_INNER_VAR_FIRST;
							break;
						case OUTER_VAR:
							scratch.opcode = EEOP_OUTER_VAR_FIRST;
							break;

							/* INDEX_VAR is handled by default case */

						default:
							scratch.opcode = EEOP_SCAN_VAR_FIRST;
							break;
					}
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Const:
			{
				Const	   *con = (Const *) node;

				scratch.opcode = EEOP_CONST;
				scratch.d.constval.value = con->constvalue;
				scratch.d.constval.isnull = con->constisnull;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Param:
			{
				Param	   *param = (Param *) node;

				switch (param->paramkind)
				{
					case PARAM_EXEC:
						scratch.opcode = EEOP_PARAM_EXEC;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;
						break;
					case PARAM_EXTERN:
						scratch.opcode = EEOP_PARAM_EXTERN;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;
						break;
					default:
						elog(ERROR, "unrecognized paramkind: %d",
							 (int) param->paramkind);
						break;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;
				AggrefExprState *astate = makeNode(AggrefExprState);

				scratch.opcode = EEOP_AGGREF;
				scratch.d.aggref.astate = astate;
				astate->aggref = aggref;

				if (parent && IsA(parent, AggState))
				{
					AggState   *aggstate = (AggState *) parent;

					aggstate->aggs = lcons(astate, aggstate->aggs);
					aggstate->numaggs++;
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "Aggref found in non-Agg plan node");
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_GroupingFunc:
			{
				GroupingFunc *grp_node = (GroupingFunc *) node;
				Agg		   *agg;

				if (!parent || !IsA(parent, AggState) ||
					!IsA(parent->plan, Agg))
					elog(ERROR, "GroupingFunc found in non-Agg plan node");

				scratch.opcode = EEOP_GROUPING_FUNC;
				scratch.d.grouping_func.parent = (AggState *) parent;

				agg = (Agg *) (parent->plan);

				if (agg->groupingSets)
					scratch.d.grouping_func.clauses = grp_node->cols;
				else
					scratch.d.grouping_func.clauses = NIL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_WindowFunc:
			{
				WindowFunc *wfunc = (WindowFunc *) node;
				WindowFuncExprState *wfstate = makeNode(WindowFuncExprState);

				wfstate->wfunc = wfunc;

				if (parent && IsA(parent, WindowAggState))
				{
					WindowAggState *winstate = (WindowAggState *) parent;
					int			nfuncs;

					winstate->funcs = lcons(wfstate, winstate->funcs);
					nfuncs = ++winstate->numfuncs;
					if (wfunc->winagg)
						winstate->numaggs++;

					/* for now initialize agg using old style expressions */
					wfstate->args = ExecInitExprList(wfunc->args, parent);
					wfstate->aggfilter = ExecInitExpr(wfunc->aggfilter,
													  parent);

					/*
					 * Complain if the windowfunc's arguments contain any
					 * windowfuncs; nested window functions are semantically
					 * nonsensical.  (This should have been caught earlier,
					 * but we defend against it here anyway.)
					 */
					if (nfuncs != winstate->numfuncs)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("window function calls cannot be nested")));
				}
				else
				{
					/* planner messed up */
					elog(ERROR, "WindowFunc found in non-WindowAgg plan node");
				}

				scratch.opcode = EEOP_WINDOW_FUNC;
				scratch.d.window_func.wfstate = wfstate;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayRef:
			{
				ArrayRef   *aref = (ArrayRef *) node;

				ExecInitArrayRef(&scratch, aref, parent, state, resv, resnull);
				break;
			}

		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;

				ExecInitFunc(&scratch, node,
							 func->args, func->funcid, func->inputcollid,
							 parent, state);
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;

				ExecInitFunc(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 parent, state);
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_DistinctExpr:
			{
				DistinctExpr *op = (DistinctExpr *) node;

				ExecInitFunc(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 parent, state);

				/*
				 * Change opcode of call instruction to EEOP_DISTINCT.
				 *
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation.  If
				 * we decided to do that here, we'd probably want separate
				 * opcodes for FUSAGE or not.
				 */
				scratch.opcode = EEOP_DISTINCT;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_NullIfExpr:
			{
				NullIfExpr *op = (NullIfExpr *) node;

				ExecInitFunc(&scratch, node,
							 op->args, op->opfuncid, op->inputcollid,
							 parent, state);

				/*
				 * Change opcode of call instruction to EEOP_NULLIF.
				 *
				 * XXX: historically we've not called the function usage
				 * pgstat infrastructure - that seems inconsistent given that
				 * we do so for normal function *and* operator evaluation.  If
				 * we decided to do that here, we'd probably want separate
				 * opcodes for FUSAGE or not.
				 */
				scratch.opcode = EEOP_NULLIF;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *opexpr = (ScalarArrayOpExpr *) node;
				Expr	   *scalararg;
				Expr	   *arrayarg;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;
				AclResult	aclresult;

				Assert(list_length(opexpr->args) == 2);
				scalararg = (Expr *) linitial(opexpr->args);
				arrayarg = (Expr *) lsecond(opexpr->args);

				/* Check permission to call function */
				aclresult = pg_proc_aclcheck(opexpr->opfuncid,
											 GetUserId(),
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(opexpr->opfuncid));
				InvokeFunctionExecuteHook(opexpr->opfuncid);

				/* Set up the primary fmgr lookup information */
				finfo = palloc0(sizeof(FmgrInfo));
				fcinfo = palloc0(sizeof(FunctionCallInfoData));
				fmgr_info(opexpr->opfuncid, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 opexpr->inputcollid, NULL, NULL);

				/* Evaluate scalar directly into left function argument */
				ExecInitExprRec(scalararg, parent, state,
								&fcinfo->arg[0], &fcinfo->argnull[0]);

				/*
				 * Evaluate array argument into our return value.  There's no
				 * danger in that, because the return value is guaranteed to
				 * be overwritten by EEOP_SCALARARRAYOP, and will not be
				 * passed to any other expression.
				 */
				ExecInitExprRec(arrayarg, parent, state, resv, resnull);

				/* And perform the operation */
				scratch.opcode = EEOP_SCALARARRAYOP;
				scratch.d.scalararrayop.element_type = InvalidOid;
				scratch.d.scalararrayop.useOr = opexpr->useOr;
				scratch.d.scalararrayop.finfo = finfo;
				scratch.d.scalararrayop.fcinfo_data = fcinfo;
				scratch.d.scalararrayop.fn_addr = finfo->fn_addr;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;
				int			nargs = list_length(boolexpr->args);
				List	   *adjust_jumps = NIL;
				int			off;
				ListCell   *lc;

				/* allocate scratch memory used by all steps of AND/OR */
				if (boolexpr->boolop != NOT_EXPR)
					scratch.d.boolexpr.anynull = (bool *) palloc(sizeof(bool));

				/*
				 * For each argument evaluate the argument itself, then
				 * perform the bool operation's appropriate handling.
				 *
				 * We can evaluate each argument into our result area, since
				 * the short-circuiting logic means we only need to remember
				 * previous NULL values.
				 *
				 * AND/OR is split into separate STEP_FIRST (one) / STEP (zero
				 * or more) / STEP_LAST (one) steps, as each of those has to
				 * perform different work.  The FIRST/LAST split is valid
				 * because AND/OR have at least two arguments.
				 */
				off = 0;
				foreach(lc, boolexpr->args)
				{
					Expr	   *arg = (Expr *) lfirst(lc);

					/* Evaluate argument into our output variable */
					ExecInitExprRec(arg, parent, state, resv, resnull);

					/* Perform the appropriate step type */
					switch (boolexpr->boolop)
					{
						case AND_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_AND_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_AND_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_AND_STEP;
							break;
						case OR_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_BOOL_OR_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_BOOL_OR_STEP_LAST;
							else
								scratch.opcode = EEOP_BOOL_OR_STEP;
							break;
						case NOT_EXPR:
							Assert(nargs == 1);

							scratch.opcode = EEOP_BOOL_NOT_STEP;
							break;
						default:
							elog(ERROR, "unrecognized boolop: %d",
								 (int) boolexpr->boolop);
							break;
					}

					scratch.d.boolexpr.jumpdone = -1;
					ExprEvalPushStep(state, &scratch);
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
					off++;
				}

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->d.boolexpr.jumpdone == -1);
					as->d.boolexpr.jumpdone = state->steps_len;
				}

				break;
			}

		case T_SubPlan:
			{
				SubPlan    *subplan = (SubPlan *) node;
				SubPlanState *sstate;

				if (!parent)
					elog(ERROR, "SubPlan found with no parent plan");

				sstate = ExecInitSubPlan(subplan, parent);

				/* add SubPlanState nodes to parent->subPlan */
				parent->subPlan = lappend(parent->subPlan, sstate);

				scratch.opcode = EEOP_SUBPLAN;
				scratch.d.subplan.sstate = sstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_AlternativeSubPlan:
			{
				AlternativeSubPlan *asplan = (AlternativeSubPlan *) node;
				AlternativeSubPlanState *asstate;

				if (!parent)
					elog(ERROR, "AlternativeSubPlan found with no parent plan");

				asstate = ExecInitAlternativeSubPlan(asplan, parent);

				scratch.opcode = EEOP_ALTERNATIVE_SUBPLAN;
				scratch.d.alternative_subplan.asstate = asstate;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_FieldSelect:
			{
				FieldSelect *fselect = (FieldSelect *) node;

				/* evaluate row/record argument into result area */
				ExecInitExprRec(fselect->arg, parent, state, resv, resnull);

				/* and extract field */
				scratch.opcode = EEOP_FIELDSELECT;
				scratch.d.fieldselect.fieldnum = fselect->fieldnum;
				scratch.d.fieldselect.resulttype = fselect->resulttype;
				scratch.d.fieldselect.argdesc = NULL;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_FieldStore:
			{
				FieldStore *fstore = (FieldStore *) node;
				TupleDesc	tupDesc;
				TupleDesc  *descp;
				Datum	   *values;
				bool	   *nulls;
				int			ncolumns;
				ListCell   *l1,
						   *l2;

				/* find out the number of columns in the composite type */
				tupDesc = lookup_rowtype_tupdesc(fstore->resulttype, -1);
				ncolumns = tupDesc->natts;
				DecrTupleDescRefCount(tupDesc);

				/* create workspace for column values */
				values = (Datum *) palloc(sizeof(Datum) * ncolumns);
				nulls = (bool *) palloc(sizeof(bool) * ncolumns);

				/* create workspace for runtime tupdesc cache */
				descp = (TupleDesc *) palloc(sizeof(TupleDesc));
				*descp = NULL;

				/* emit code to evaluate the composite input value */
				ExecInitExprRec(fstore->arg, parent, state, resv, resnull);

				/* next, deform the input tuple into our workspace */
				scratch.opcode = EEOP_FIELDSTORE_DEFORM;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;
				scratch.d.fieldstore.ncolumns = ncolumns;
				ExprEvalPushStep(state, &scratch);

				/* evaluate new field values, store in workspace columns */
				forboth(l1, fstore->newvals, l2, fstore->fieldnums)
				{
					Expr	   *e = (Expr *) lfirst(l1);
					AttrNumber	fieldnum = lfirst_int(l2);
					Datum	   *save_innermost_caseval;
					bool	   *save_innermost_casenull;

					if (fieldnum <= 0 || fieldnum > ncolumns)
						elog(ERROR, "field number %d is out of range in FieldStore",
							 fieldnum);

					/*
					 * Use the CaseTestExpr mechanism to pass down the old
					 * value of the field being replaced; this is needed in
					 * case the newval is itself a FieldStore or ArrayRef that
					 * has to obtain and modify the old value.  It's safe to
					 * reuse the CASE mechanism because there cannot be a CASE
					 * between here and where the value would be needed, and a
					 * field assignment can't be within a CASE either.  (So
					 * saving and restoring innermost_caseval is just
					 * paranoia, but let's do it anyway.)
					 *
					 * Another non-obvious point is that it's safe to use the
					 * field's values[]/nulls[] entries as both the caseval
					 * source and the result address for this subexpression.
					 * That's okay only because (1) both FieldStore and
					 * ArrayRef evaluate their arg or refexpr inputs first,
					 * and (2) any such CaseTestExpr is directly the arg or
					 * refexpr input.  So any read of the caseval will occur
					 * before there's a chance to overwrite it.  Also, if
					 * multiple entries in the newvals/fieldnums lists target
					 * the same field, they'll effectively be applied
					 * left-to-right which is what we want.
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = &values[fieldnum - 1];
					state->innermost_casenull = &nulls[fieldnum - 1];

					ExecInitExprRec(e, parent, state,
									&values[fieldnum - 1],
									&nulls[fieldnum - 1]);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;
				}

				/* finally, form result tuple */
				scratch.opcode = EEOP_FIELDSTORE_FORM;
				scratch.d.fieldstore.fstore = fstore;
				scratch.d.fieldstore.argdesc = descp;
				scratch.d.fieldstore.values = values;
				scratch.d.fieldstore.nulls = nulls;
				scratch.d.fieldstore.ncolumns = ncolumns;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RelabelType:
			{
				/* relabel doesn't need to do anything at runtime */
				RelabelType *relabel = (RelabelType *) node;

				ExecInitExprRec(relabel->arg, parent, state, resv, resnull);
				break;
			}

		case T_CoerceViaIO:
			{
				CoerceViaIO *iocoerce = (CoerceViaIO *) node;
				Oid			iofunc;
				bool		typisvarlena;
				Oid			typioparam;
				FunctionCallInfo fcinfo_in;

				/* evaluate argument into step's result area */
				ExecInitExprRec(iocoerce->arg, parent, state, resv, resnull);

				/*
				 * Prepare both output and input function calls, to be
				 * evaluated inside a single evaluation step for speed - this
				 * can be a very common operation.
				 *
				 * We don't check permissions here as a type's input/output
				 * function are assumed to be executable by everyone.
				 */
				scratch.opcode = EEOP_IOCOERCE;

				/* lookup the source type's output function */
				scratch.d.iocoerce.finfo_out = palloc0(sizeof(FmgrInfo));
				scratch.d.iocoerce.fcinfo_data_out = palloc0(sizeof(FunctionCallInfoData));

				getTypeOutputInfo(exprType((Node *) iocoerce->arg),
								  &iofunc, &typisvarlena);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_out);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_out);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_out,
										 scratch.d.iocoerce.finfo_out,
										 1, InvalidOid, NULL, NULL);

				/* lookup the result type's input function */
				scratch.d.iocoerce.finfo_in = palloc0(sizeof(FmgrInfo));
				scratch.d.iocoerce.fcinfo_data_in = palloc0(sizeof(FunctionCallInfoData));

				getTypeInputInfo(iocoerce->resulttype,
								 &iofunc, &typioparam);
				fmgr_info(iofunc, scratch.d.iocoerce.finfo_in);
				fmgr_info_set_expr((Node *) node, scratch.d.iocoerce.finfo_in);
				InitFunctionCallInfoData(*scratch.d.iocoerce.fcinfo_data_in,
										 scratch.d.iocoerce.finfo_in,
										 3, InvalidOid, NULL, NULL);

				/*
				 * We can preload the second and third arguments for the input
				 * function, since they're constants.
				 */
				fcinfo_in = scratch.d.iocoerce.fcinfo_data_in;
				fcinfo_in->arg[1] = ObjectIdGetDatum(typioparam);
				fcinfo_in->argnull[1] = false;
				fcinfo_in->arg[2] = Int32GetDatum(-1);
				fcinfo_in->argnull[2] = false;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *acoerce = (ArrayCoerceExpr *) node;
				Oid			resultelemtype;

				/* evaluate argument into step's result area */
				ExecInitExprRec(acoerce->arg, parent, state, resv, resnull);

				resultelemtype = get_element_type(acoerce->resulttype);
				if (!OidIsValid(resultelemtype))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("target type is not an array")));
				/* Arrays over domains aren't supported yet */
				Assert(getBaseType(resultelemtype) == resultelemtype);

				scratch.opcode = EEOP_ARRAYCOERCE;
				scratch.d.arraycoerce.coerceexpr = acoerce;
				scratch.d.arraycoerce.resultelemtype = resultelemtype;

				if (OidIsValid(acoerce->elemfuncid))
				{
					AclResult	aclresult;

					/* Check permission to call function */
					aclresult = pg_proc_aclcheck(acoerce->elemfuncid,
												 GetUserId(),
												 ACL_EXECUTE);
					if (aclresult != ACLCHECK_OK)
						aclcheck_error(aclresult, ACL_KIND_PROC,
									   get_func_name(acoerce->elemfuncid));
					InvokeFunctionExecuteHook(acoerce->elemfuncid);

					/* Set up the primary fmgr lookup information */
					scratch.d.arraycoerce.elemfunc =
						(FmgrInfo *) palloc0(sizeof(FmgrInfo));
					fmgr_info(acoerce->elemfuncid,
							  scratch.d.arraycoerce.elemfunc);
					fmgr_info_set_expr((Node *) acoerce,
									   scratch.d.arraycoerce.elemfunc);

					/* Set up workspace for array_map */
					scratch.d.arraycoerce.amstate =
						(ArrayMapState *) palloc0(sizeof(ArrayMapState));
				}
				else
				{
					/* Don't need workspace if there's no conversion func */
					scratch.d.arraycoerce.elemfunc = NULL;
					scratch.d.arraycoerce.amstate = NULL;
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ConvertRowtypeExpr:
			{
				ConvertRowtypeExpr *convert = (ConvertRowtypeExpr *) node;

				/* evaluate argument into step's result area */
				ExecInitExprRec(convert->arg, parent, state, resv, resnull);

				/* and push conversion step */
				scratch.opcode = EEOP_CONVERT_ROWTYPE;
				scratch.d.convert_rowtype.convert = convert;
				scratch.d.convert_rowtype.indesc = NULL;
				scratch.d.convert_rowtype.outdesc = NULL;
				scratch.d.convert_rowtype.map = NULL;
				scratch.d.convert_rowtype.initialized = false;

				ExprEvalPushStep(state, &scratch);
				break;
			}

			/* note that CaseWhen expressions are handled within this block */
		case T_CaseExpr:
			{
				CaseExpr   *caseExpr = (CaseExpr *) node;
				List	   *adjust_jumps = NIL;
				Datum	   *caseval = NULL;
				bool	   *casenull = NULL;
				ListCell   *lc;

				/*
				 * If there's a test expression, we have to evaluate it and
				 * save the value where the CaseTestExpr placeholders can find
				 * it.
				 */
				if (caseExpr->arg != NULL)
				{
					/* Evaluate testexpr into caseval/casenull workspace */
					caseval = palloc(sizeof(Datum));
					casenull = palloc(sizeof(bool));

					ExecInitExprRec(caseExpr->arg, parent, state,
									caseval, casenull);

					/*
					 * Since value might be read multiple times, force to R/O
					 * - but only if it could be an expanded datum.
					 */
					if (get_typlen(exprType((Node *) caseExpr->arg)) == -1)
					{
						/* change caseval in-place */
						scratch.opcode = EEOP_MAKE_READONLY;
						scratch.resvalue = caseval;
						scratch.resnull = casenull;
						scratch.d.make_readonly.value = caseval;
						scratch.d.make_readonly.isnull = casenull;
						ExprEvalPushStep(state, &scratch);
						/* restore normal settings of scratch fields */
						scratch.resvalue = resv;
						scratch.resnull = resnull;
					}
				}

				/*
				 * Prepare to evaluate each of the WHEN clauses in turn; as
				 * soon as one is true we return the value of the
				 * corresponding THEN clause.  If none are true then we return
				 * the value of the ELSE clause, or NULL if there is none.
				 */
				foreach(lc, caseExpr->args)
				{
					CaseWhen   *when = (CaseWhen *) lfirst(lc);
					Datum	   *save_innermost_caseval;
					bool	   *save_innermost_casenull;
					int			whenstep;

					/*
					 * Make testexpr result available to CaseTestExpr nodes
					 * within the condition.  We must save and restore prior
					 * setting of innermost_caseval fields, in case this node
					 * is itself within a larger CASE.
					 *
					 * If there's no test expression, we don't actually need
					 * to save and restore these fields; but it's less code to
					 * just do so unconditionally.
					 */
					save_innermost_caseval = state->innermost_caseval;
					save_innermost_casenull = state->innermost_casenull;
					state->innermost_caseval = caseval;
					state->innermost_casenull = casenull;

					/* evaluate condition into CASE's result variables */
					ExecInitExprRec(when->expr, parent, state, resv, resnull);

					state->innermost_caseval = save_innermost_caseval;
					state->innermost_casenull = save_innermost_casenull;

					/* If WHEN result isn't true, jump to next CASE arm */
					scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
					scratch.d.jump.jumpdone = -1;	/* computed later */
					ExprEvalPushStep(state, &scratch);
					whenstep = state->steps_len - 1;

					/*
					 * If WHEN result is true, evaluate THEN result, storing
					 * it into the CASE's result variables.
					 */
					ExecInitExprRec(when->result, parent, state, resv, resnull);

					/* Emit JUMP step to jump to end of CASE's code */
					scratch.opcode = EEOP_JUMP;
					scratch.d.jump.jumpdone = -1;	/* computed later */
					ExprEvalPushStep(state, &scratch);

					/*
					 * Don't know address for that jump yet, compute once the
					 * whole CASE expression is built.
					 */
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);

					/*
					 * But we can set WHEN test's jump target now, to make it
					 * jump to the next WHEN subexpression or the ELSE.
					 */
					state->steps[whenstep].d.jump.jumpdone = state->steps_len;
				}

				/* transformCaseExpr always adds a default */
				Assert(caseExpr->defresult);

				/* evaluate ELSE expr into CASE's result variables */
				ExecInitExprRec(caseExpr->defresult, parent, state,
								resv, resnull);

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}

		case T_CaseTestExpr:
			{
				/*
				 * Read from location identified by innermost_caseval.  Note
				 * that innermost_caseval could be NULL, if this node isn't
				 * actually within a CASE structure; some parts of the system
				 * abuse CaseTestExpr to cause a read of a value externally
				 * supplied in econtext->caseValue_datum.  We'll take care of
				 * that scenario at runtime.
				 */
				scratch.opcode = EEOP_CASE_TESTVAL;
				scratch.d.casetest.value = state->innermost_caseval;
				scratch.d.casetest.isnull = state->innermost_casenull;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_ArrayExpr:
			{
				ArrayExpr  *arrayexpr = (ArrayExpr *) node;
				int			nelems = list_length(arrayexpr->elements);
				ListCell   *lc;
				int			elemoff;

				/*
				 * Evaluate by computing each element, and then forming the
				 * array.  Elements are computed into scratch arrays
				 * associated with the ARRAYEXPR step.
				 */
				scratch.opcode = EEOP_ARRAYEXPR;
				scratch.d.arrayexpr.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.arrayexpr.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.arrayexpr.nelems = nelems;

				/* fill remaining fields of step */
				scratch.d.arrayexpr.multidims = arrayexpr->multidims;
				scratch.d.arrayexpr.elemtype = arrayexpr->element_typeid;

				/* do one-time catalog lookup for type info */
				get_typlenbyvalalign(arrayexpr->element_typeid,
									 &scratch.d.arrayexpr.elemlength,
									 &scratch.d.arrayexpr.elembyval,
									 &scratch.d.arrayexpr.elemalign);

				/* prepare to evaluate all arguments */
				elemoff = 0;
				foreach(lc, arrayexpr->elements)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, parent, state,
									&scratch.d.arrayexpr.elemvalues[elemoff],
									&scratch.d.arrayexpr.elemnulls[elemoff]);
					elemoff++;
				}

				/* and then collect all into an array */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RowExpr:
			{
				RowExpr    *rowexpr = (RowExpr *) node;
				int			nelems = list_length(rowexpr->args);
				TupleDesc	tupdesc;
				int			i;
				ListCell   *l;

				/* Build tupdesc to describe result tuples */
				if (rowexpr->row_typeid == RECORDOID)
				{
					/* generic record, use types of given expressions */
					tupdesc = ExecTypeFromExprList(rowexpr->args);
				}
				else
				{
					/* it's been cast to a named type, use that */
					tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
				}
				/* In either case, adopt RowExpr's column aliases */
				ExecTypeSetColNames(tupdesc, rowexpr->colnames);
				/* Bless the tupdesc in case it's now of type RECORD */
				BlessTupleDesc(tupdesc);

				/*
				 * In the named-type case, the tupdesc could have more columns
				 * than are in the args list, since the type might have had
				 * columns added since the ROW() was parsed.  We want those
				 * extra columns to go to nulls, so we make sure that the
				 * workspace arrays are large enough and then initialize any
				 * extra columns to read as NULLs.
				 */
				Assert(nelems <= tupdesc->natts);
				nelems = Max(nelems, tupdesc->natts);

				/*
				 * Evaluate by first building datums for each field, and then
				 * a final step forming the composite datum.
				 */
				scratch.opcode = EEOP_ROW;
				scratch.d.row.tupdesc = tupdesc;

				/* space for the individual field datums */
				scratch.d.row.elemvalues =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.row.elemnulls =
					(bool *) palloc(sizeof(bool) * nelems);
				/* as explained above, make sure any extra columns are null */
				memset(scratch.d.row.elemnulls, true, sizeof(bool) * nelems);

				/* Set up evaluation, skipping any deleted columns */
				i = 0;
				foreach(l, rowexpr->args)
				{
					Form_pg_attribute att = TupleDescAttr(tupdesc, i);
					Expr	   *e = (Expr *) lfirst(l);

					if (!att->attisdropped)
					{
						/*
						 * Guard against ALTER COLUMN TYPE on rowtype since
						 * the RowExpr was created.  XXX should we check
						 * typmod too?	Not sure we can be sure it'll be the
						 * same.
						 */
						if (exprType((Node *) e) != att->atttypid)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("ROW() column has type %s instead of type %s",
											format_type_be(exprType((Node *) e)),
											format_type_be(att->atttypid))));
					}
					else
					{
						/*
						 * Ignore original expression and insert a NULL. We
						 * don't really care what type of NULL it is, so
						 * always make an int4 NULL.
						 */
						e = (Expr *) makeNullConst(INT4OID, -1, InvalidOid);
					}

					/* Evaluate column expr into appropriate workspace slot */
					ExecInitExprRec(e, parent, state,
									&scratch.d.row.elemvalues[i],
									&scratch.d.row.elemnulls[i]);
					i++;
				}

				/* And finally build the row value */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_RowCompareExpr:
			{
				RowCompareExpr *rcexpr = (RowCompareExpr *) node;
				int			nopers = list_length(rcexpr->opnos);
				List	   *adjust_jumps = NIL;
				ListCell   *l_left_expr,
						   *l_right_expr,
						   *l_opno,
						   *l_opfamily,
						   *l_inputcollid;
				ListCell   *lc;
				int			off;

				/*
				 * Iterate over each field, prepare comparisons.  To handle
				 * NULL results, prepare jumps to after the expression.  If a
				 * comparison yields a != 0 result, jump to the final step.
				 */
				Assert(list_length(rcexpr->largs) == nopers);
				Assert(list_length(rcexpr->rargs) == nopers);
				Assert(list_length(rcexpr->opfamilies) == nopers);
				Assert(list_length(rcexpr->inputcollids) == nopers);

				off = 0;
				for (off = 0,
					 l_left_expr = list_head(rcexpr->largs),
					 l_right_expr = list_head(rcexpr->rargs),
					 l_opno = list_head(rcexpr->opnos),
					 l_opfamily = list_head(rcexpr->opfamilies),
					 l_inputcollid = list_head(rcexpr->inputcollids);
					 off < nopers;
					 off++,
					 l_left_expr = lnext(l_left_expr),
					 l_right_expr = lnext(l_right_expr),
					 l_opno = lnext(l_opno),
					 l_opfamily = lnext(l_opfamily),
					 l_inputcollid = lnext(l_inputcollid))
				{
					Expr	   *left_expr = (Expr *) lfirst(l_left_expr);
					Expr	   *right_expr = (Expr *) lfirst(l_right_expr);
					Oid			opno = lfirst_oid(l_opno);
					Oid			opfamily = lfirst_oid(l_opfamily);
					Oid			inputcollid = lfirst_oid(l_inputcollid);
					int			strategy;
					Oid			lefttype;
					Oid			righttype;
					Oid			proc;
					FmgrInfo   *finfo;
					FunctionCallInfo fcinfo;

					get_op_opfamily_properties(opno, opfamily, false,
											   &strategy,
											   &lefttype,
											   &righttype);
					proc = get_opfamily_proc(opfamily,
											 lefttype,
											 righttype,
											 BTORDER_PROC);
					if (!OidIsValid(proc))
						elog(ERROR, "missing support function %d(%u,%u) in opfamily %u",
							 BTORDER_PROC, lefttype, righttype, opfamily);

					/* Set up the primary fmgr lookup information */
					finfo = palloc0(sizeof(FmgrInfo));
					fcinfo = palloc0(sizeof(FunctionCallInfoData));
					fmgr_info(proc, finfo);
					fmgr_info_set_expr((Node *) node, finfo);
					InitFunctionCallInfoData(*fcinfo, finfo, 2,
											 inputcollid, NULL, NULL);

					/*
					 * If we enforced permissions checks on index support
					 * functions, we'd need to make a check here.  But the
					 * index support machinery doesn't do that, and thus
					 * neither does this code.
					 */

					/* evaluate left and right args directly into fcinfo */
					ExecInitExprRec(left_expr, parent, state,
									&fcinfo->arg[0], &fcinfo->argnull[0]);
					ExecInitExprRec(right_expr, parent, state,
									&fcinfo->arg[1], &fcinfo->argnull[1]);

					scratch.opcode = EEOP_ROWCOMPARE_STEP;
					scratch.d.rowcompare_step.finfo = finfo;
					scratch.d.rowcompare_step.fcinfo_data = fcinfo;
					scratch.d.rowcompare_step.fn_addr = finfo->fn_addr;
					/* jump targets filled below */
					scratch.d.rowcompare_step.jumpnull = -1;
					scratch.d.rowcompare_step.jumpdone = -1;

					ExprEvalPushStep(state, &scratch);
					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
				}

				/*
				 * We could have a zero-column rowtype, in which case the rows
				 * necessarily compare equal.
				 */
				if (nopers == 0)
				{
					scratch.opcode = EEOP_CONST;
					scratch.d.constval.value = Int32GetDatum(0);
					scratch.d.constval.isnull = false;
					ExprEvalPushStep(state, &scratch);
				}

				/* Finally, examine the last comparison result */
				scratch.opcode = EEOP_ROWCOMPARE_FINAL;
				scratch.d.rowcompare_final.rctype = rcexpr->rctype;
				ExprEvalPushStep(state, &scratch);

				/* adjust jump targetss */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_ROWCOMPARE_STEP);
					Assert(as->d.rowcompare_step.jumpdone == -1);
					Assert(as->d.rowcompare_step.jumpnull == -1);

					/* jump to comparison evaluation */
					as->d.rowcompare_step.jumpdone = state->steps_len - 1;
					/* jump to the following expression */
					as->d.rowcompare_step.jumpnull = state->steps_len;
				}

				break;
			}

		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				List	   *adjust_jumps = NIL;
				ListCell   *lc;

				/* We assume there's at least one arg */
				Assert(coalesce->args != NIL);

				/*
				 * Prepare evaluation of all coalesced arguments, after each
				 * one push a step that short-circuits if not null.
				 */
				foreach(lc, coalesce->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					/* evaluate argument, directly into result datum */
					ExecInitExprRec(e, parent, state, resv, resnull);

					/* if it's not null, skip to end of COALESCE expr */
					scratch.opcode = EEOP_JUMP_IF_NOT_NULL;
					scratch.d.jump.jumpdone = -1;	/* adjust later */
					ExprEvalPushStep(state, &scratch);

					adjust_jumps = lappend_int(adjust_jumps,
											   state->steps_len - 1);
				}

				/*
				 * No need to add a constant NULL return - we only can get to
				 * the end of the expression if a NULL already is being
				 * returned.
				 */

				/* adjust jump targets */
				foreach(lc, adjust_jumps)
				{
					ExprEvalStep *as = &state->steps[lfirst_int(lc)];

					Assert(as->opcode == EEOP_JUMP_IF_NOT_NULL);
					Assert(as->d.jump.jumpdone == -1);
					as->d.jump.jumpdone = state->steps_len;
				}

				break;
			}

		case T_MinMaxExpr:
			{
				MinMaxExpr *minmaxexpr = (MinMaxExpr *) node;
				int			nelems = list_length(minmaxexpr->args);
				TypeCacheEntry *typentry;
				FmgrInfo   *finfo;
				FunctionCallInfo fcinfo;
				ListCell   *lc;
				int			off;

				/* Look up the btree comparison function for the datatype */
				typentry = lookup_type_cache(minmaxexpr->minmaxtype,
											 TYPECACHE_CMP_PROC);
				if (!OidIsValid(typentry->cmp_proc))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("could not identify a comparison function for type %s",
									format_type_be(minmaxexpr->minmaxtype))));

				/*
				 * If we enforced permissions checks on index support
				 * functions, we'd need to make a check here.  But the index
				 * support machinery doesn't do that, and thus neither does
				 * this code.
				 */

				/* Perform function lookup */
				finfo = palloc0(sizeof(FmgrInfo));
				fcinfo = palloc0(sizeof(FunctionCallInfoData));
				fmgr_info(typentry->cmp_proc, finfo);
				fmgr_info_set_expr((Node *) node, finfo);
				InitFunctionCallInfoData(*fcinfo, finfo, 2,
										 minmaxexpr->inputcollid, NULL, NULL);

				scratch.opcode = EEOP_MINMAX;
				/* allocate space to store arguments */
				scratch.d.minmax.values =
					(Datum *) palloc(sizeof(Datum) * nelems);
				scratch.d.minmax.nulls =
					(bool *) palloc(sizeof(bool) * nelems);
				scratch.d.minmax.nelems = nelems;

				scratch.d.minmax.op = minmaxexpr->op;
				scratch.d.minmax.finfo = finfo;
				scratch.d.minmax.fcinfo_data = fcinfo;

				/* evaluate expressions into minmax->values/nulls */
				off = 0;
				foreach(lc, minmaxexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(lc);

					ExecInitExprRec(e, parent, state,
									&scratch.d.minmax.values[off],
									&scratch.d.minmax.nulls[off]);
					off++;
				}

				/* and push the final comparison */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_SQLValueFunction:
			{
				SQLValueFunction *svf = (SQLValueFunction *) node;

				scratch.opcode = EEOP_SQLVALUEFUNCTION;
				scratch.d.sqlvaluefunction.svf = svf;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_XmlExpr:
			{
				XmlExpr    *xexpr = (XmlExpr *) node;
				int			nnamed = list_length(xexpr->named_args);
				int			nargs = list_length(xexpr->args);
				int			off;
				ListCell   *arg;

				scratch.opcode = EEOP_XMLEXPR;
				scratch.d.xmlexpr.xexpr = xexpr;

				/* allocate space for storing all the arguments */
				if (nnamed)
				{
					scratch.d.xmlexpr.named_argvalue =
						(Datum *) palloc(sizeof(Datum) * nnamed);
					scratch.d.xmlexpr.named_argnull =
						(bool *) palloc(sizeof(bool) * nnamed);
				}
				else
				{
					scratch.d.xmlexpr.named_argvalue = NULL;
					scratch.d.xmlexpr.named_argnull = NULL;
				}

				if (nargs)
				{
					scratch.d.xmlexpr.argvalue =
						(Datum *) palloc(sizeof(Datum) * nargs);
					scratch.d.xmlexpr.argnull =
						(bool *) palloc(sizeof(bool) * nargs);
				}
				else
				{
					scratch.d.xmlexpr.argvalue = NULL;
					scratch.d.xmlexpr.argnull = NULL;
				}

				/* prepare argument execution */
				off = 0;
				foreach(arg, xexpr->named_args)
				{
					Expr	   *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, parent, state,
									&scratch.d.xmlexpr.named_argvalue[off],
									&scratch.d.xmlexpr.named_argnull[off]);
					off++;
				}

				off = 0;
				foreach(arg, xexpr->args)
				{
					Expr	   *e = (Expr *) lfirst(arg);

					ExecInitExprRec(e, parent, state,
									&scratch.d.xmlexpr.argvalue[off],
									&scratch.d.xmlexpr.argnull[off]);
					off++;
				}

				/* and evaluate the actual XML expression */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_NullTest:
			{
				NullTest   *ntest = (NullTest *) node;

				if (ntest->nulltesttype == IS_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNULL;
				}
				else if (ntest->nulltesttype == IS_NOT_NULL)
				{
					if (ntest->argisrow)
						scratch.opcode = EEOP_NULLTEST_ROWISNOTNULL;
					else
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
				}
				else
				{
					elog(ERROR, "unrecognized nulltesttype: %d",
						 (int) ntest->nulltesttype);
				}
				/* initialize cache in case it's a row test */
				scratch.d.nulltest_row.argdesc = NULL;

				/* first evaluate argument into result variable */
				ExecInitExprRec(ntest->arg, parent, state,
								resv, resnull);

				/* then push the test of that argument */
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_BooleanTest:
			{
				BooleanTest *btest = (BooleanTest *) node;

				/*
				 * Evaluate argument, directly into result datum.  That's ok,
				 * because resv/resnull is definitely not used anywhere else,
				 * and will get overwritten by the below EEOP_BOOLTEST_IS_*
				 * step.
				 */
				ExecInitExprRec(btest->arg, parent, state, resv, resnull);

				switch (btest->booltesttype)
				{
					case IS_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_TRUE;
						break;
					case IS_NOT_TRUE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_TRUE;
						break;
					case IS_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_FALSE;
						break;
					case IS_NOT_FALSE:
						scratch.opcode = EEOP_BOOLTEST_IS_NOT_FALSE;
						break;
					case IS_UNKNOWN:
						/* Same as scalar IS NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNULL;
						break;
					case IS_NOT_UNKNOWN:
						/* Same as scalar IS NOT NULL test */
						scratch.opcode = EEOP_NULLTEST_ISNOTNULL;
						break;
					default:
						elog(ERROR, "unrecognized booltesttype: %d",
							 (int) btest->booltesttype);
				}

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_CoerceToDomain:
			{
				CoerceToDomain *ctest = (CoerceToDomain *) node;

				ExecInitCoerceToDomain(&scratch, ctest, parent, state,
									   resv, resnull);
				break;
			}

		case T_CoerceToDomainValue:
			{
				/*
				 * Read from location identified by innermost_domainval.  Note
				 * that innermost_domainval could be NULL, if we're compiling
				 * a standalone domain check rather than one embedded in a
				 * larger expression.  In that case we must read from
				 * econtext->domainValue_datum.  We'll take care of that
				 * scenario at runtime.
				 */
				scratch.opcode = EEOP_DOMAIN_TESTVAL;
				/* we share instruction union variant with case testval */
				scratch.d.casetest.value = state->innermost_domainval;
				scratch.d.casetest.isnull = state->innermost_domainnull;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_CurrentOfExpr:
			{
				scratch.opcode = EEOP_CURRENTOFEXPR;
				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_NextValueExpr:
			{
				NextValueExpr *nve = (NextValueExpr *) node;

				scratch.opcode = EEOP_NEXTVALUEEXPR;
				scratch.d.nextvalueexpr.seqid = nve->seqid;
				scratch.d.nextvalueexpr.seqtypid = nve->typeId;

				ExprEvalPushStep(state, &scratch);
				break;
			}

		case T_JsonExpr:
			{
				JsonExpr   *jexpr = castNode(JsonExpr, node);
				ListCell   *argexprlc;
				ListCell   *argnamelc;

				scratch.opcode = EEOP_JSONEXPR;
				scratch.d.jsonexpr.jsexpr = jexpr;

				scratch.d.jsonexpr.raw_expr =
						palloc(sizeof(*scratch.d.jsonexpr.raw_expr));

				ExecInitExprRec((Expr *) jexpr->raw_expr, parent, state,
								&scratch.d.jsonexpr.raw_expr->value,
								&scratch.d.jsonexpr.raw_expr->isnull);

				scratch.d.jsonexpr.formatted_expr =
						ExecInitExpr((Expr *) jexpr->formatted_expr, parent);

				scratch.d.jsonexpr.result_expr =
						ExecInitExpr((Expr *) jexpr->result_expr, parent);

				scratch.d.jsonexpr.default_on_empty =
					ExecInitExpr((Expr *) jexpr->on_empty.default_expr, parent);

				scratch.d.jsonexpr.default_on_error =
					ExecInitExpr((Expr *) jexpr->on_error.default_expr, parent);

				if (jexpr->coerce_via_io || jexpr->omit_quotes)
				{
					Oid			typinput;

					/* lookup the result type's input function */
					getTypeInputInfo(jexpr->returning.typid, &typinput,
									 &scratch.d.jsonexpr.input.typioparam);
					fmgr_info(typinput, &scratch.d.jsonexpr.input.func);
				}

				scratch.d.jsonexpr.args = NIL;

				forboth(argexprlc, jexpr->passing.values,
						argnamelc, jexpr->passing.names)
				{
					Expr	   *argexpr = (Expr *) lfirst(argexprlc);
					Value	   *argname = (Value *) lfirst(argnamelc);
					JsonPathVariableEvalContext *var = palloc(sizeof(*var));

					var->var.varName = cstring_to_text(argname->val.str);
					var->var.typid = exprType((Node *) argexpr);
					var->var.typmod = exprTypmod((Node *) argexpr);
					var->var.cb = EvalJsonPathVar;
					var->var.cb_arg = var;
					var->estate = ExecInitExpr(argexpr, parent);
					var->econtext = NULL;
					var->evaluated = false;
					var->value = (Datum) 0;
					var->isnull = true;

					scratch.d.jsonexpr.args =
						lappend(scratch.d.jsonexpr.args, var);
				}

				memset(&scratch.d.jsonexpr.scalar, 0,
					   sizeof(scratch.d.jsonexpr.scalar));

				ExprEvalPushStep(state, &scratch);
			}
			break;


		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Add another expression evaluation step to ExprState->steps.
 *
 * Note that this potentially re-allocates es->steps, therefore no pointer
 * into that array may be used while the expression is still being built.
 */
static void
ExprEvalPushStep(ExprState *es, const ExprEvalStep *s)
{
	if (es->steps_alloc == 0)
	{
		es->steps_alloc = 16;
		es->steps = palloc(sizeof(ExprEvalStep) * es->steps_alloc);
	}
	else if (es->steps_alloc == es->steps_len)
	{
		es->steps_alloc *= 2;
		es->steps = repalloc(es->steps,
							 sizeof(ExprEvalStep) * es->steps_alloc);
	}

	memcpy(&es->steps[es->steps_len++], s, sizeof(ExprEvalStep));
}

/*
 * Perform setup necessary for the evaluation of a function-like expression,
 * appending argument evaluation steps to the steps list in *state, and
 * setting up *scratch so it is ready to be pushed.
 *
 * *scratch is not pushed here, so that callers may override the opcode,
 * which is useful for function-like cases like DISTINCT.
 */
static void
ExecInitFunc(ExprEvalStep *scratch, Expr *node, List *args, Oid funcid,
			 Oid inputcollid, PlanState *parent, ExprState *state)
{
	int			nargs = list_length(args);
	AclResult	aclresult;
	FmgrInfo   *flinfo;
	FunctionCallInfo fcinfo;
	int			argno;
	ListCell   *lc;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(funcid));
	InvokeFunctionExecuteHook(funcid);

	/*
	 * Safety check on nargs.  Under normal circumstances this should never
	 * fail, as parser should check sooner.  But possibly it might fail if
	 * server has been compiled with FUNC_MAX_ARGS smaller than some functions
	 * declared in pg_proc?
	 */
	if (nargs > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS)));

	/* Allocate function lookup data and parameter workspace for this call */
	scratch->d.func.finfo = palloc0(sizeof(FmgrInfo));
	scratch->d.func.fcinfo_data = palloc0(sizeof(FunctionCallInfoData));
	flinfo = scratch->d.func.finfo;
	fcinfo = scratch->d.func.fcinfo_data;

	/* Set up the primary fmgr lookup information */
	fmgr_info(funcid, flinfo);
	fmgr_info_set_expr((Node *) node, flinfo);

	/* Initialize function call parameter structure too */
	InitFunctionCallInfoData(*fcinfo, flinfo,
							 nargs, inputcollid, NULL, NULL);

	/* Keep extra copies of this info to save an indirection at runtime */
	scratch->d.func.fn_addr = flinfo->fn_addr;
	scratch->d.func.nargs = nargs;

	/* We only support non-set functions here */
	if (flinfo->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set"),
				 parent ? executor_errposition(parent->state,
											   exprLocation((Node *) node)) : 0));

	/* Build code to evaluate arguments directly into the fcinfo struct */
	argno = 0;
	foreach(lc, args)
	{
		Expr	   *arg = (Expr *) lfirst(lc);

		if (IsA(arg, Const))
		{
			/*
			 * Don't evaluate const arguments every round; especially
			 * interesting for constants in comparisons.
			 */
			Const	   *con = (Const *) arg;

			fcinfo->arg[argno] = con->constvalue;
			fcinfo->argnull[argno] = con->constisnull;
		}
		else
		{
			ExecInitExprRec(arg, parent, state,
							&fcinfo->arg[argno], &fcinfo->argnull[argno]);
		}
		argno++;
	}

	/* Insert appropriate opcode depending on strictness and stats level */
	if (pgstat_track_functions <= flinfo->fn_stats)
	{
		if (flinfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT;
		else
			scratch->opcode = EEOP_FUNCEXPR;
	}
	else
	{
		if (flinfo->fn_strict && nargs > 0)
			scratch->opcode = EEOP_FUNCEXPR_STRICT_FUSAGE;
		else
			scratch->opcode = EEOP_FUNCEXPR_FUSAGE;
	}
}

/*
 * Add expression steps deforming the ExprState's inner/outer/scan slots
 * as much as required by the expression.
 */
static void
ExecInitExprSlots(ExprState *state, Node *node)
{
	LastAttnumInfo info = {0, 0, 0};
	ExprEvalStep scratch;

	/*
	 * Figure out which attributes we're going to need.
	 */
	get_last_attnums_walker(node, &info);

	/* Emit steps as needed */
	if (info.last_inner > 0)
	{
		scratch.opcode = EEOP_INNER_FETCHSOME;
		scratch.d.fetch.last_var = info.last_inner;
		ExprEvalPushStep(state, &scratch);
	}
	if (info.last_outer > 0)
	{
		scratch.opcode = EEOP_OUTER_FETCHSOME;
		scratch.d.fetch.last_var = info.last_outer;
		ExprEvalPushStep(state, &scratch);
	}
	if (info.last_scan > 0)
	{
		scratch.opcode = EEOP_SCAN_FETCHSOME;
		scratch.d.fetch.last_var = info.last_scan;
		ExprEvalPushStep(state, &scratch);
	}
}

/*
 * get_last_attnums_walker: expression walker for ExecInitExprSlots
 */
static bool
get_last_attnums_walker(Node *node, LastAttnumInfo *info)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *variable = (Var *) node;
		AttrNumber	attnum = variable->varattno;

		switch (variable->varno)
		{
			case INNER_VAR:
				info->last_inner = Max(info->last_inner, attnum);
				break;

			case OUTER_VAR:
				info->last_outer = Max(info->last_outer, attnum);
				break;

				/* INDEX_VAR is handled by default case */

			default:
				info->last_scan = Max(info->last_scan, attnum);
				break;
		}
		return false;
	}

	/*
	 * Don't examine the arguments or filters of Aggrefs or WindowFuncs,
	 * because those do not represent expressions to be evaluated within the
	 * calling expression's econtext.  GroupingFunc arguments are never
	 * evaluated at all.
	 */
	if (IsA(node, Aggref))
		return false;
	if (IsA(node, WindowFunc))
		return false;
	if (IsA(node, GroupingFunc))
		return false;
	return expression_tree_walker(node, get_last_attnums_walker,
								  (void *) info);
}

/*
 * Prepare step for the evaluation of a whole-row variable.
 * The caller still has to push the step.
 */
static void
ExecInitWholeRowVar(ExprEvalStep *scratch, Var *variable, PlanState *parent)
{
	/* fill in all but the target */
	scratch->opcode = EEOP_WHOLEROW;
	scratch->d.wholerow.var = variable;
	scratch->d.wholerow.first = true;
	scratch->d.wholerow.slow = false;
	scratch->d.wholerow.tupdesc = NULL; /* filled at runtime */
	scratch->d.wholerow.junkFilter = NULL;

	/*
	 * If the input tuple came from a subquery, it might contain "resjunk"
	 * columns (such as GROUP BY or ORDER BY columns), which we don't want to
	 * keep in the whole-row result.  We can get rid of such columns by
	 * passing the tuple through a JunkFilter --- but to make one, we have to
	 * lay our hands on the subquery's targetlist.  Fortunately, there are not
	 * very many cases where this can happen, and we can identify all of them
	 * by examining our parent PlanState.  We assume this is not an issue in
	 * standalone expressions that don't have parent plans.  (Whole-row Vars
	 * can occur in such expressions, but they will always be referencing
	 * table rows.)
	 */
	if (parent)
	{
		PlanState  *subplan = NULL;

		switch (nodeTag(parent))
		{
			case T_SubqueryScanState:
				subplan = ((SubqueryScanState *) parent)->subplan;
				break;
			case T_CteScanState:
				subplan = ((CteScanState *) parent)->cteplanstate;
				break;
			default:
				break;
		}

		if (subplan)
		{
			bool		junk_filter_needed = false;
			ListCell   *tlist;

			/* Detect whether subplan tlist actually has any junk columns */
			foreach(tlist, subplan->plan->targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tlist);

				if (tle->resjunk)
				{
					junk_filter_needed = true;
					break;
				}
			}

			/* If so, build the junkfilter now */
			if (junk_filter_needed)
			{
				scratch->d.wholerow.junkFilter =
					ExecInitJunkFilter(subplan->plan->targetlist,
									   ExecGetResultType(subplan)->tdhasoid,
									   ExecInitExtraTupleSlot(parent->state));
			}
		}
	}
}

/*
 * Prepare evaluation of an ArrayRef expression.
 */
static void
ExecInitArrayRef(ExprEvalStep *scratch, ArrayRef *aref, PlanState *parent,
				 ExprState *state, Datum *resv, bool *resnull)
{
	bool		isAssignment = (aref->refassgnexpr != NULL);
	ArrayRefState *arefstate = palloc0(sizeof(ArrayRefState));
	List	   *adjust_jumps = NIL;
	ListCell   *lc;
	int			i;

	/* Fill constant fields of ArrayRefState */
	arefstate->isassignment = isAssignment;
	arefstate->refelemtype = aref->refelemtype;
	arefstate->refattrlength = get_typlen(aref->refarraytype);
	get_typlenbyvalalign(aref->refelemtype,
						 &arefstate->refelemlength,
						 &arefstate->refelembyval,
						 &arefstate->refelemalign);

	/*
	 * Evaluate array input.  It's safe to do so into resv/resnull, because we
	 * won't use that as target for any of the other subexpressions, and it'll
	 * be overwritten by the final EEOP_ARRAYREF_FETCH/ASSIGN step, which is
	 * pushed last.
	 */
	ExecInitExprRec(aref->refexpr, parent, state, resv, resnull);

	/*
	 * If refexpr yields NULL, and it's a fetch, then result is NULL.  We can
	 * implement this with just JUMP_IF_NULL, since we evaluated the array
	 * into the desired target location.
	 */
	if (!isAssignment)
	{
		scratch->opcode = EEOP_JUMP_IF_NULL;
		scratch->d.jump.jumpdone = -1;	/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_jumps = lappend_int(adjust_jumps,
								   state->steps_len - 1);
	}

	/* Verify subscript list lengths are within limit */
	if (list_length(aref->refupperindexpr) > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						list_length(aref->refupperindexpr), MAXDIM)));

	if (list_length(aref->reflowerindexpr) > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						list_length(aref->reflowerindexpr), MAXDIM)));

	/* Evaluate upper subscripts */
	i = 0;
	foreach(lc, aref->refupperindexpr)
	{
		Expr	   *e = (Expr *) lfirst(lc);

		/* When slicing, individual subscript bounds can be omitted */
		if (!e)
		{
			arefstate->upperprovided[i] = false;
			i++;
			continue;
		}

		arefstate->upperprovided[i] = true;

		/* Each subscript is evaluated into subscriptvalue/subscriptnull */
		ExecInitExprRec(e, parent, state,
						&arefstate->subscriptvalue, &arefstate->subscriptnull);

		/* ... and then ARRAYREF_SUBSCRIPT saves it into step's workspace */
		scratch->opcode = EEOP_ARRAYREF_SUBSCRIPT;
		scratch->d.arrayref_subscript.state = arefstate;
		scratch->d.arrayref_subscript.off = i;
		scratch->d.arrayref_subscript.isupper = true;
		scratch->d.arrayref_subscript.jumpdone = -1;	/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_jumps = lappend_int(adjust_jumps,
								   state->steps_len - 1);
		i++;
	}
	arefstate->numupper = i;

	/* Evaluate lower subscripts similarly */
	i = 0;
	foreach(lc, aref->reflowerindexpr)
	{
		Expr	   *e = (Expr *) lfirst(lc);

		/* When slicing, individual subscript bounds can be omitted */
		if (!e)
		{
			arefstate->lowerprovided[i] = false;
			i++;
			continue;
		}

		arefstate->lowerprovided[i] = true;

		/* Each subscript is evaluated into subscriptvalue/subscriptnull */
		ExecInitExprRec(e, parent, state,
						&arefstate->subscriptvalue, &arefstate->subscriptnull);

		/* ... and then ARRAYREF_SUBSCRIPT saves it into step's workspace */
		scratch->opcode = EEOP_ARRAYREF_SUBSCRIPT;
		scratch->d.arrayref_subscript.state = arefstate;
		scratch->d.arrayref_subscript.off = i;
		scratch->d.arrayref_subscript.isupper = false;
		scratch->d.arrayref_subscript.jumpdone = -1;	/* adjust later */
		ExprEvalPushStep(state, scratch);
		adjust_jumps = lappend_int(adjust_jumps,
								   state->steps_len - 1);
		i++;
	}
	arefstate->numlower = i;

	/* Should be impossible if parser is sane, but check anyway: */
	if (arefstate->numlower != 0 &&
		arefstate->numupper != arefstate->numlower)
		elog(ERROR, "upper and lower index lists are not same length");

	if (isAssignment)
	{
		Datum	   *save_innermost_caseval;
		bool	   *save_innermost_casenull;

		/*
		 * We might have a nested-assignment situation, in which the
		 * refassgnexpr is itself a FieldStore or ArrayRef that needs to
		 * obtain and modify the previous value of the array element or slice
		 * being replaced.  If so, we have to extract that value from the
		 * array and pass it down via the CaseTestExpr mechanism.  It's safe
		 * to reuse the CASE mechanism because there cannot be a CASE between
		 * here and where the value would be needed, and an array assignment
		 * can't be within a CASE either.  (So saving and restoring
		 * innermost_caseval is just paranoia, but let's do it anyway.)
		 *
		 * Since fetching the old element might be a nontrivial expense, do it
		 * only if the argument actually needs it.
		 */
		if (isAssignmentIndirectionExpr(aref->refassgnexpr))
		{
			scratch->opcode = EEOP_ARRAYREF_OLD;
			scratch->d.arrayref.state = arefstate;
			ExprEvalPushStep(state, scratch);
		}

		/* ARRAYREF_OLD puts extracted value into prevvalue/prevnull */
		save_innermost_caseval = state->innermost_caseval;
		save_innermost_casenull = state->innermost_casenull;
		state->innermost_caseval = &arefstate->prevvalue;
		state->innermost_casenull = &arefstate->prevnull;

		/* evaluate replacement value into replacevalue/replacenull */
		ExecInitExprRec(aref->refassgnexpr, parent, state,
						&arefstate->replacevalue, &arefstate->replacenull);

		state->innermost_caseval = save_innermost_caseval;
		state->innermost_casenull = save_innermost_casenull;

		/* and perform the assignment */
		scratch->opcode = EEOP_ARRAYREF_ASSIGN;
		scratch->d.arrayref.state = arefstate;
		ExprEvalPushStep(state, scratch);
	}
	else
	{
		/* array fetch is much simpler */
		scratch->opcode = EEOP_ARRAYREF_FETCH;
		scratch->d.arrayref.state = arefstate;
		ExprEvalPushStep(state, scratch);
	}

	/* adjust jump targets */
	foreach(lc, adjust_jumps)
	{
		ExprEvalStep *as = &state->steps[lfirst_int(lc)];

		if (as->opcode == EEOP_ARRAYREF_SUBSCRIPT)
		{
			Assert(as->d.arrayref_subscript.jumpdone == -1);
			as->d.arrayref_subscript.jumpdone = state->steps_len;
		}
		else
		{
			Assert(as->opcode == EEOP_JUMP_IF_NULL);
			Assert(as->d.jump.jumpdone == -1);
			as->d.jump.jumpdone = state->steps_len;
		}
	}
}

/*
 * Helper for preparing ArrayRef expressions for evaluation: is expr a nested
 * FieldStore or ArrayRef that needs the old element value passed down?
 *
 * (We could use this in FieldStore too, but in that case passing the old
 * value is so cheap there's no need.)
 *
 * Note: it might seem that this needs to recurse, but it does not; the
 * CaseTestExpr, if any, will be directly the arg or refexpr of the top-level
 * node.  Nested-assignment situations give rise to expression trees in which
 * each level of assignment has its own CaseTestExpr, and the recursive
 * structure appears within the newvals or refassgnexpr field.
 */
static bool
isAssignmentIndirectionExpr(Expr *expr)
{
	if (expr == NULL)
		return false;			/* just paranoia */
	if (IsA(expr, FieldStore))
	{
		FieldStore *fstore = (FieldStore *) expr;

		if (fstore->arg && IsA(fstore->arg, CaseTestExpr))
			return true;
	}
	else if (IsA(expr, ArrayRef))
	{
		ArrayRef   *arrayRef = (ArrayRef *) expr;

		if (arrayRef->refexpr && IsA(arrayRef->refexpr, CaseTestExpr))
			return true;
	}
	return false;
}

/*
 * Prepare evaluation of a CoerceToDomain expression.
 */
static void
ExecInitCoerceToDomain(ExprEvalStep *scratch, CoerceToDomain *ctest,
					   PlanState *parent, ExprState *state,
					   Datum *resv, bool *resnull)
{
	ExprEvalStep scratch2;
	DomainConstraintRef *constraint_ref;
	Datum	   *domainval = NULL;
	bool	   *domainnull = NULL;
	Datum	   *save_innermost_domainval;
	bool	   *save_innermost_domainnull;
	ListCell   *l;

	scratch->d.domaincheck.resulttype = ctest->resulttype;
	/* we'll allocate workspace only if needed */
	scratch->d.domaincheck.checkvalue = NULL;
	scratch->d.domaincheck.checknull = NULL;

	/*
	 * Evaluate argument - it's fine to directly store it into resv/resnull,
	 * if there's constraint failures there'll be errors, otherwise it's what
	 * needs to be returned.
	 */
	ExecInitExprRec(ctest->arg, parent, state, resv, resnull);

	/*
	 * Note: if the argument is of varlena type, it could be a R/W expanded
	 * object.  We want to return the R/W pointer as the final result, but we
	 * have to pass a R/O pointer as the value to be tested by any functions
	 * in check expressions.  We don't bother to emit a MAKE_READONLY step
	 * unless there's actually at least one check expression, though.  Until
	 * we've tested that, domainval/domainnull are NULL.
	 */

	/*
	 * Collect the constraints associated with the domain.
	 *
	 * Note: before PG v10 we'd recheck the set of constraints during each
	 * evaluation of the expression.  Now we bake them into the ExprState
	 * during executor initialization.  That means we don't need typcache.c to
	 * provide compiled exprs.
	 */
	constraint_ref = (DomainConstraintRef *)
		palloc(sizeof(DomainConstraintRef));
	InitDomainConstraintRef(ctest->resulttype,
							constraint_ref,
							CurrentMemoryContext,
							false);

	/*
	 * Compile code to check each domain constraint.  NOTNULL constraints can
	 * just be applied on the resv/resnull value, but for CHECK constraints we
	 * need more pushups.
	 */
	foreach(l, constraint_ref->constraints)
	{
		DomainConstraintState *con = (DomainConstraintState *) lfirst(l);

		scratch->d.domaincheck.constraintname = con->name;

		switch (con->constrainttype)
		{
			case DOM_CONSTRAINT_NOTNULL:
				scratch->opcode = EEOP_DOMAIN_NOTNULL;
				ExprEvalPushStep(state, scratch);
				break;
			case DOM_CONSTRAINT_CHECK:
				/* Allocate workspace for CHECK output if we didn't yet */
				if (scratch->d.domaincheck.checkvalue == NULL)
				{
					scratch->d.domaincheck.checkvalue =
						(Datum *) palloc(sizeof(Datum));
					scratch->d.domaincheck.checknull =
						(bool *) palloc(sizeof(bool));
				}

				/*
				 * If first time through, determine where CoerceToDomainValue
				 * nodes should read from.
				 */
				if (domainval == NULL)
				{
					/*
					 * Since value might be read multiple times, force to R/O
					 * - but only if it could be an expanded datum.
					 */
					if (get_typlen(ctest->resulttype) == -1)
					{
						/* Yes, so make output workspace for MAKE_READONLY */
						domainval = (Datum *) palloc(sizeof(Datum));
						domainnull = (bool *) palloc(sizeof(bool));

						/* Emit MAKE_READONLY */
						scratch2.opcode = EEOP_MAKE_READONLY;
						scratch2.resvalue = domainval;
						scratch2.resnull = domainnull;
						scratch2.d.make_readonly.value = resv;
						scratch2.d.make_readonly.isnull = resnull;
						ExprEvalPushStep(state, &scratch2);
					}
					else
					{
						/* No, so it's fine to read from resv/resnull */
						domainval = resv;
						domainnull = resnull;
					}
				}

				/*
				 * Set up value to be returned by CoerceToDomainValue nodes.
				 * We must save and restore innermost_domainval/null fields,
				 * in case this node is itself within a check expression for
				 * another domain.
				 */
				save_innermost_domainval = state->innermost_domainval;
				save_innermost_domainnull = state->innermost_domainnull;
				state->innermost_domainval = domainval;
				state->innermost_domainnull = domainnull;

				/* evaluate check expression value */
				ExecInitExprRec(con->check_expr, parent, state,
								scratch->d.domaincheck.checkvalue,
								scratch->d.domaincheck.checknull);

				state->innermost_domainval = save_innermost_domainval;
				state->innermost_domainnull = save_innermost_domainnull;

				/* now test result */
				scratch->opcode = EEOP_DOMAIN_CHECK;
				ExprEvalPushStep(state, scratch);

				break;
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->constrainttype);
				break;
		}
	}
}
