package com.aegis.query;

import com.aegis.query.parser.AQLLexer;
import com.aegis.query.parser.AQLParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * AQL (AEGIS Query Language) Transpiler
 * Parses AQL queries and transpiles them to tier-specific query formats
 */
@Component
public class AqlTranspiler {

    @Autowired
    private OpenSearchTranspiler openSearchTranspiler;

    @Autowired
    private ClickHouseTranspiler clickHouseTranspiler;

    /**
     * Transpile an AQL query string to a QueryPlan
     * 
     * @param aqlQuery The AQL query string
     * @return QueryPlan containing tier-specific sub-queries
     */
    public QueryPlan transpile(String aqlQuery) {
        // Initialize ANTLR lexer
        AQLLexer lexer = new AQLLexer(CharStreams.fromString(aqlQuery));
        
        // Create token stream
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        
        // Initialize ANTLR parser
        AQLParser parser = new AQLParser(tokens);
        
        // Parse the query to AST
        ParseTree tree = parser.query();
        
        // Create visitor to extract query context
        AqlVisitor visitor = new AqlVisitor();
        QueryContext context = visitor.visit(tree);
        
        // Build query plan from context
        return buildQueryPlan(context);
    }

    /**
     * Build a QueryPlan from the parsed QueryContext
     */
    private QueryPlan buildQueryPlan(QueryContext context) {
        QueryPlan plan = new QueryPlan();
        
        // Determine which storage tiers to query based on time range
        TierSelection tierSelection = determineTiers(context.getTimeRange());
        
        // Generate tier-specific sub-queries
        if (tierSelection.includesHotTier()) {
            plan.addSubQuery(transpileToOpenSearch(context));
        }
        
        if (tierSelection.includesWarmTier()) {
            plan.addSubQuery(transpileToClickHouse(context));
        }
        
        if (tierSelection.includesColdTier()) {
            plan.addSubQuery(transpileToIceberg(context));
        }
        
        return plan;
    }

    /**
     * Determine which storage tiers to query based on time range
     */
    private TierSelection determineTiers(TimeRange timeRange) {
        // Hot tier: last 7 days
        // Warm tier: 8-90 days
        // Cold tier: 90+ days
        
        long now = System.currentTimeMillis();
        long sevenDaysAgo = now - (7L * 24 * 60 * 60 * 1000);
        long ninetyDaysAgo = now - (90L * 24 * 60 * 60 * 1000);
        
        boolean includesHot = timeRange.getLatest() >= sevenDaysAgo;
        boolean includesWarm = timeRange.getEarliest() < sevenDaysAgo && timeRange.getLatest() >= ninetyDaysAgo;
        boolean includesCold = timeRange.getEarliest() < ninetyDaysAgo;
        
        return new TierSelection(includesHot, includesWarm, includesCold);
    }

    /**
     * Transpile to OpenSearch DSL for hot tier
     */
    private SubQuery transpileToOpenSearch(QueryContext context) {
        return new OpenSearchQuery(openSearchTranspiler.transpileToOpenSearch(context));
    }

    /**
     * Transpile to ClickHouse SQL for warm tier
     */
    private SubQuery transpileToClickHouse(QueryContext context) {
        return new ClickHouseQuery(clickHouseTranspiler.transpileToClickHouse(context));
    }

    /**
     * Transpile to Iceberg SQL for cold tier
     */
    private SubQuery transpileToIceberg(QueryContext context) {
        // Implementation in Phase 17
        return null;
    }

    /**
     * Helper class for tier selection
     */
    private static class TierSelection {
        private final boolean hot;
        private final boolean warm;
        private final boolean cold;

        public TierSelection(boolean hot, boolean warm, boolean cold) {
            this.hot = hot;
            this.warm = warm;
            this.cold = cold;
        }

        public boolean includesHotTier() { return hot; }
        public boolean includesWarmTier() { return warm; }
        public boolean includesColdTier() { return cold; }
    }
}
