/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.query;

import org.apache.atlas.query.SelectClauseMetadata.AggregatorFlag;
import org.apache.atlas.query.antlr4.AtlasDSLParser.*;
import org.apache.atlas.query.antlr4.AtlasDSLParserBaseVisitor;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DSLVisitor extends AtlasDSLParserBaseVisitor<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(DSLVisitor.class);

    private static final String AND = "AND";
    private static final String OR  = "OR";

    private final GremlinQueryComposer     gremlinQueryComposer;
    private final AtlasDSL.QueryMetadata   queryMetadata;
    private       GroupByExpressionContext groupByExpressionContext;

    public DSLVisitor(GremlinQueryComposer gremlinQueryComposer, AtlasDSL.QueryMetadata queryMetadata) {
        this.gremlinQueryComposer = gremlinQueryComposer;
        this.queryMetadata = queryMetadata;
    }

    public Void visitLimitOffset(LimitOffsetContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitLimitOffset({})", ctx);
        }

        LimitClauseContext  limitClause  = ctx.limitClause();
        String              limit        = limitClause.NUMBER().getText();
        OffsetClauseContext offsetClause = ctx.offsetClause();
        String              offset       = offsetClause == null ? "0" : offsetClause.NUMBER().getText();

        SelectClauseMetadata selectClauseMetadata = gremlinQueryComposer.getSelectClause();
        // Limit offset shouldn't be applied when the select clause only has aggregators
        if (selectClauseMetadata != null && selectClauseMetadata.onlyAggregators()) {
            return null;
        }

        if (groupByExpressionContext != null) {
            // For groupBy the result is a Map, offset and limit won't work, better to defer the paging to the projection util
            gremlinQueryComposer.addLimit(limit, "0", true);
        } else {
            gremlinQueryComposer.addLimit(limit, offset, true);
        }

        return super.visitLimitOffset(ctx);
    }

    @Override
    public Void visitSelectClause(final SelectClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DSLVisitor.visitSelectClause({})",ctx);
        }

        // Select can have only attributes, aliased attributes or aggregate functions
        List<SelectExpressionContext> selectExpressionContexts = ctx.selectExpr().selectExpression();
        SelectClauseMetadata          selectClauseMetadata     = new SelectClauseMetadata(selectExpressionContexts.size());


            for (int i = 0; i < selectExpressionContexts.size(); i++) {
                SelectExpressionContext selectExpression = selectExpressionContexts.get(i);
                CountClauseContext      countClause      = selectExpression.expr().compE().countClause();
                SumClauseContext        sumClause        = selectExpression.expr().compE().sumClause();
                MinClauseContext        minClause        = selectExpression.expr().compE().minClause();
                MaxClauseContext        maxClause        = selectExpression.expr().compE().maxClause();
                IdentifierContext       identifier       = selectExpression.identifier();

                selectClauseMetadata.setLabel(i,identifier != null ? identifier.getText() : selectExpression.getText());

                if (Objects.nonNull(countClause)) {
                    selectClauseMetadata.setItem(i,"count");
                    selectClauseMetadata.setAggregatorFlag(i, AggregatorFlag.COUNT);
                } else if (Objects.nonNull(sumClause)) {
                    selectClauseMetadata.setItem(i, sumClause.expr().getText());
                    selectClauseMetadata.setAggregatorFlag(i, AggregatorFlag.SUM);
                } else if (Objects.nonNull(minClause)) {
                    selectClauseMetadata.setItem(i, minClause.expr().getText());
                    selectClauseMetadata.setAggregatorFlag(i, AggregatorFlag.MIN);
                } else if (Objects.nonNull(maxClause)) {
                    selectClauseMetadata.setItem(i, maxClause.expr().getText());
                    selectClauseMetadata.setAggregatorFlag(i, AggregatorFlag.MAX);
                } else {
                    selectClauseMetadata.setItem(i, selectExpression.expr().getText());
                    selectClauseMetadata.setAggregatorFlag(i, AggregatorFlag.NONE);
                }
            }

        gremlinQueryComposer.addSelect(selectClauseMetadata);

        // Only process groupBy expression if orderBy is not present, else defer until orderBy visit
        if (!queryMetadata.hasOrderBy() && queryMetadata.hasGroupBy()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing groupBy expression as well");
            }
            String s = groupByExpressionContext.selectExpr().getText();
            gremlinQueryComposer.addGroupBy(s);
        }

        return super.visitSelectClause(ctx);
    }

    @Override
    public Void visitOrderByExpr(OrderByExprContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitOrderByExpr({})", ctx);
        }

        // Extract the attribute from parentheses
        String text = ctx.expr().getText().replace("(", "").replace(")", "");
        gremlinQueryComposer.addOrderBy(text, (ctx.sortOrder() != null && ctx.sortOrder().getText().equalsIgnoreCase("desc")));

        if (queryMetadata.hasGroupBy()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing groupBy expression as well");
            }
            String s = groupByExpressionContext.selectExpr().getText();
            gremlinQueryComposer.addGroupBy(s);
        }

        return super.visitOrderByExpr(ctx);
    }

    @Override
    public Void visitWhereClause(WhereClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitWhereClause({})", ctx);
        }
        LOG.info("=> DSLVisitor.visitWhereClause({})", ctx);

        ExprContext expr = ctx.expr();
        processExpr(expr, gremlinQueryComposer);
        return super.visitWhereClause(ctx);
    }

    @Override
    public Void visitFromExpression(final FromExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitFromExpression({})", ctx);
        }

        FromSrcContext   fromSrc   = ctx.fromSrc();
        AliasExprContext aliasExpr = fromSrc.aliasExpr();

        if (aliasExpr != null) {
            gremlinQueryComposer.addFromAlias(aliasExpr.identifier(0).getText(), aliasExpr.identifier(1).getText());
        } else {
            if (fromSrc.identifier() != null) {
                gremlinQueryComposer.addFrom(fromSrc.identifier().getText());
            } else {
                gremlinQueryComposer.addFrom(fromSrc.literal().getText());
            }
        }
        return super.visitFromExpression(ctx);
    }

    @Override
    public Void visitSingleQrySrc(SingleQrySrcContext ctx) {
        if (ctx.fromExpression() == null) {
            if (ctx.expr() != null && !gremlinQueryComposer.hasFromClause()) {
                inferFromClause(ctx);
            }

            if (ctx.expr() != null && gremlinQueryComposer.hasFromClause()) {
                processExpr(ctx.expr(), gremlinQueryComposer);
            }
        }

        return super.visitSingleQrySrc(ctx);
    }

    @Override
    public Void visitGroupByExpression(GroupByExpressionContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitGroupByExpression({})", ctx);
        }

        groupByExpressionContext = ctx;
        if (queryMetadata.hasOrderBy() || queryMetadata.hasSelect()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deferring groupBy processing, orderBy = {} and select = {}", queryMetadata.hasOrderBy(), queryMetadata.hasSelect());
            }
            return null;
        } else {
            String s = ctx.selectExpr().getText();
            gremlinQueryComposer.addGroupBy(s);
            return super.visitGroupByExpression(ctx);
        }
    }

    private Void visitIsClause(GremlinQueryComposer gqc, IsClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitIsClause({})", ctx);
        }

        gqc.addIsA(ctx.arithE().getText(), ctx.identifier().getText());
        return super.visitIsClause(ctx);
    }

    private void visitHasClause(GremlinQueryComposer gqc, HasClauseContext ctx) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("=> DSLVisitor.visitHasClause({})", ctx);
        }

        gqc.addFromProperty(ctx.arithE().getText(), ctx.identifier().getText());
        super.visitHasClause(ctx);
    }

    private void inferFromClause(SingleQrySrcContext ctx) {
        if (ctx.fromExpression() != null) {
            return;
        }

        if (ctx.expr() != null && gremlinQueryComposer.hasFromClause()) {
            return;
        }

        if (ctx.expr().compE() != null && ctx.expr().compE().isClause() != null && ctx.expr().compE().isClause().arithE() != null) {
            gremlinQueryComposer.addFrom(ctx.expr().compE().isClause().arithE().getText());
            return;
        }

        if (ctx.expr().compE() != null && ctx.expr().compE().hasClause() != null && ctx.expr().compE().hasClause().arithE() != null) {
            gremlinQueryComposer.addFrom(ctx.expr().compE().hasClause().arithE().getText());
        }
    }

    private void processExpr(final ExprContext expr, GremlinQueryComposer gremlinQueryComposer) {
        if (CollectionUtils.isNotEmpty(expr.exprRight())) {
            processExprRight(expr, gremlinQueryComposer);
        } else {
            processExpr(expr.compE(), gremlinQueryComposer);
        }
    }

    private void processExprRight(final ExprContext expr, GremlinQueryComposer gremlinQueryComposer) {
        GremlinQueryComposer nestedProcessor = gremlinQueryComposer.createNestedProcessor();
        List<AtlasGraphTraversal> nestedTraversals = new ArrayList<>();
        String                    prev             = null;

        // Process first expression then proceed with the others
        // expr -> compE exprRight*
        processExpr(expr.compE(), nestedProcessor);
        nestedTraversals.add(nestedProcessor.getGraphTraversal());

        // Record all processed attributes
        gremlinQueryComposer.addProcessedAttributes(nestedProcessor.getAttributesProcessed());

        for (ExprRightContext exprRight : expr.exprRight()) {
            nestedProcessor = gremlinQueryComposer.createNestedProcessor();

            // AND expression
            if (exprRight.K_AND() != null) {
                if (prev == null) prev = AND;
                if (OR.equalsIgnoreCase(prev)) {
                    // Change of context
                    GremlinQueryComposer orClause = nestedProcessor.createNestedProcessor();
                    AtlasGraphTraversal  orTraversal = orClause.getGraphTraversal();
                    orTraversal.or(nestedTraversals.toArray(new Traversal[0]));
                    nestedTraversals.clear();
                    nestedTraversals.add(orTraversal);

                    // Record all processed attributes
                    gremlinQueryComposer.addProcessedAttributes(orClause.getAttributesProcessed());
                }
                prev = AND;
            }
            // OR expression
            if (exprRight.K_OR() != null) {
                if (prev == null) prev = OR;
                if (AND.equalsIgnoreCase(prev)) {
                    // Change of context
                    GremlinQueryComposer andClause = nestedProcessor.createNestedProcessor();
                    AtlasGraphTraversal  andTraversal = andClause.getGraphTraversal();
                    andTraversal.and(nestedTraversals.toArray(new Traversal[0]));
                    nestedTraversals.clear();
                    nestedTraversals.add(andTraversal);

                    // Record all processed attributes
                    gremlinQueryComposer.addProcessedAttributes(andClause.getAttributesProcessed());
                }
                prev = OR;
            }
            processExpr(exprRight.compE(), nestedProcessor);
            nestedTraversals.add(nestedProcessor.getGraphTraversal());

            // Record all processed attributes
            gremlinQueryComposer.addProcessedAttributes(nestedProcessor.getAttributesProcessed());
        }
        if (AND.equalsIgnoreCase(prev)) {
            gremlinQueryComposer.addAndClauses(nestedTraversals);
        }
        if (OR.equalsIgnoreCase(prev)) {
            gremlinQueryComposer.addOrClauses(nestedTraversals);
        }
    }

    private void processExpr(final CompEContext compE, final GremlinQueryComposer gremlinQueryComposer) {
        if (compE != null && compE.isClause() == null && compE.hasClause() == null) {
            ComparisonClauseContext comparisonClause = compE.comparisonClause();

            // The nested expression might have ANDs/ORs
            if (comparisonClause == null) {
                ExprContext exprContext = compE.arithE().multiE().atomE().expr();
                // Only extract comparison clause if there are no nested exprRight clauses
                if (CollectionUtils.isEmpty(exprContext.exprRight())) {
                    comparisonClause = exprContext.compE().comparisonClause();
                }
            }

            if (comparisonClause != null) {
                String       lhs      = comparisonClause.arithE(0).getText();
                String       op, rhs;
                AtomEContext atomECtx = comparisonClause.arithE(1).multiE().atomE();
                if (atomECtx.literal() == null ||
                            (atomECtx.literal() != null && atomECtx.literal().valueArray() == null)) {
                    op = comparisonClause.operator().getText().toUpperCase();
                    rhs = comparisonClause.arithE(1).getText();
                } else {
                    op = "in";
                    rhs = getInClause(atomECtx);
                }

                gremlinQueryComposer.addWhere(lhs, op, rhs);
            } else {
                processExpr(compE.arithE().multiE().atomE().expr(), gremlinQueryComposer);
            }
        }

        if (compE != null && compE.isClause() != null) {
            visitIsClause(gremlinQueryComposer, compE.isClause());
        }

        if (compE != null && compE.hasClause() != null) {
            visitHasClause(gremlinQueryComposer, compE.hasClause());
        }
    }

    private String getInClause(AtomEContext atomEContext) {
        StringBuilder     sb                = new StringBuilder();
        ValueArrayContext valueArrayContext = atomEContext.literal().valueArray();
        int               startIdx          = 1;
        int               endIdx            = valueArrayContext.children.size() - 1;
        for (int i = startIdx; i < endIdx; i++) {
            sb.append(valueArrayContext.getChild(i));
        }

        return sb.toString();
    }
}