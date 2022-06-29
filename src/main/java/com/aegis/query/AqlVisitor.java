package com.aegis.query;

import com.aegis.query.parser.AQLBaseVisitor;
import com.aegis.query.parser.AQLParser;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * ANTLR visitor for parsing AQL queries into QueryContext
 */
public class AqlVisitor extends AQLBaseVisitor<QueryContext> {

    private QueryContext context;

    @Override
    public QueryContext visitQuery(AQLParser.QueryContext ctx) {
        context = new QueryContext();
        
        // Extract source
        if (ctx.sourceClause() != null) {
            context.setSource(extractString(ctx.sourceClause().STRING().getText()));
        }
        
        // Extract time range
        if (ctx.timeRangeClause() != null) {
            TimeRange timeRange = extractTimeRange(ctx.timeRangeClause());
            context.setTimeRange(timeRange);
        } else {
            // Default: last 24 hours
            long now = System.currentTimeMillis();
            long oneDayAgo = now - (24L * 60 * 60 * 1000);
            context.setTimeRange(new TimeRange(oneDayAgo, now));
        }
        
        // Extract where clause
        if (ctx.whereClause() != null) {
            Expression expr = extractExpression(ctx.whereClause().expression());
            context.setWhereExpression(expr);
        }
        
        // Extract stats clause
        if (ctx.statsClause() != null) {
            StatsClause stats = extractStatsClause(ctx.statsClause());
            context.setStatsClause(stats);
        }
        
        // Extract sort clause
        if (ctx.sortClause() != null) {
            SortClause sort = extractSortClause(ctx.sortClause());
            context.setSortClause(sort);
        }
        
        // Extract head clause
        if (ctx.headClause() != null) {
            int limit = Integer.parseInt(ctx.headClause().NUMBER().getText());
            context.setLimit(limit);
        }
        
        return context;
    }

    private TimeRange extractTimeRange(AQLParser.TimeRangeClauseContext ctx) {
        long earliest = parseTimeValue(ctx.timeValue(0));
        long latest = parseTimeValue(ctx.timeValue(1));
        return new TimeRange(earliest, latest);
    }

    private long parseTimeValue(AQLParser.TimeValueContext ctx) {
        if (ctx.STRING() != null) {
            // Absolute time: "2022-01-01 00:00:00"
            String timeStr = extractString(ctx.STRING().getText());
            LocalDateTime ldt = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
            // Relative time: "-7d", "-1h"
            AQLParser.RelativeTimeContext relCtx = ctx.relativeTime();
            int number = Integer.parseInt(relCtx.NUMBER().getText());
            String unit = relCtx.timeUnit().getText();
            
            long now = System.currentTimeMillis();
            long offset = switch (unit) {
                case "d" -> number * 24L * 60 * 60 * 1000;
                case "h" -> number * 60L * 60 * 1000;
                case "m" -> number * 60L * 1000;
                case "s" -> number * 1000L;
                default -> 0;
            };
            
            return now - offset;
        }
    }

    private Expression extractExpression(AQLParser.ExpressionContext ctx) {
        if (ctx instanceof AQLParser.AndExpressionContext) {
            AQLParser.AndExpressionContext andCtx = (AQLParser.AndExpressionContext) ctx;
            Expression left = extractExpression(andCtx.expression(0));
            Expression right = extractExpression(andCtx.expression(1));
            return new BinaryExpression("AND", left, right);
        } else if (ctx instanceof AQLParser.OrExpressionContext) {
            AQLParser.OrExpressionContext orCtx = (AQLParser.OrExpressionContext) ctx;
            Expression left = extractExpression(orCtx.expression(0));
            Expression right = extractExpression(orCtx.expression(1));
            return new BinaryExpression("OR", left, right);
        } else if (ctx instanceof AQLParser.ComparisonExpressionContext) {
            AQLParser.ComparisonExpressionContext compCtx = (AQLParser.ComparisonExpressionContext) ctx;
            String field = compCtx.field().getText();
            String operator = compCtx.op().getText();
            String value = extractValue(compCtx.value());
            return new ComparisonExpression(field, operator, value);
        } else if (ctx instanceof AQLParser.ContainsExpressionContext) {
            AQLParser.ContainsExpressionContext contCtx = (AQLParser.ContainsExpressionContext) ctx;
            String field = contCtx.field().getText();
            String value = extractString(contCtx.STRING().getText());
            return new ComparisonExpression(field, "contains", value);
        } else if (ctx instanceof AQLParser.ParenExpressionContext) {
            AQLParser.ParenExpressionContext parenCtx = (AQLParser.ParenExpressionContext) ctx;
            return extractExpression(parenCtx.expression());
        }
        
        return null;
    }

    private StatsClause extractStatsClause(AQLParser.StatsClauseContext ctx) {
        List<Aggregation> aggregations = new ArrayList<>();
        
        for (AQLParser.AggregationContext aggCtx : ctx.aggregationList().aggregation()) {
            String function = aggCtx.aggFunction().getText();
            String field = aggCtx.field().getText();
            String alias = aggCtx.IDENTIFIER() != null ? aggCtx.IDENTIFIER().getText() : null;
            aggregations.add(new Aggregation(function, field, alias));
        }
        
        List<String> groupByFields = new ArrayList<>();
        if (ctx.fieldList() != null) {
            for (AQLParser.FieldContext fieldCtx : ctx.fieldList().field()) {
                groupByFields.add(fieldCtx.getText());
            }
        }
        
        return new StatsClause(aggregations, groupByFields);
    }

    private SortClause extractSortClause(AQLParser.SortClauseContext ctx) {
        List<SortField> sortFields = new ArrayList<>();
        
        for (AQLParser.SortFieldContext sortFieldCtx : ctx.sortFieldList().sortField()) {
            String field = sortFieldCtx.field().getText();
            String order = sortFieldCtx.sortOrder() != null ? sortFieldCtx.sortOrder().getText() : "asc";
            sortFields.add(new SortField(field, order));
        }
        
        return new SortClause(sortFields);
    }

    private String extractValue(AQLParser.ValueContext ctx) {
        if (ctx.STRING() != null) {
            return extractString(ctx.STRING().getText());
        } else if (ctx.NUMBER() != null) {
            return ctx.NUMBER().getText();
        } else if (ctx.BOOLEAN() != null) {
            return ctx.BOOLEAN().getText();
        }
        return null;
    }

    private String extractString(String quoted) {
        // Remove surrounding quotes
        return quoted.substring(1, quoted.length() - 1);
    }
}
