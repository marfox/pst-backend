package org.wikidata.query.rdf.primarysources.common;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;

import org.json.simple.JSONObject;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A caching system for datasets statistics.
 * It stores total and missing statements and references count per dataset.
 * The cache file is serialized in JSON and looks like this:
 * <br />
 * <code>{ <br />
 * &nbsp;&nbsp; "http://a-dataset-URI": { <br />
 * &nbsp;&nbsp;&nbsp;&nbsp; "total_statements": 2001, <br />
 * &nbsp;&nbsp;&nbsp;&nbsp; "missing_statements": 666, <br />
 * &nbsp;&nbsp;&nbsp;&nbsp; "missing_references": 1984, <br />
 * &nbsp;&nbsp;&nbsp;&nbsp; "total_references": 1269 <br />
 * &nbsp;&nbsp; } <br />
 * }</code>
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Dec 20, 2017.
 */
public final class DatasetsStatisticsCache {

    private static final Logger log = LoggerFactory.getLogger(DatasetsStatisticsCache.class);

    private DatasetsStatisticsCache() {
    }

    /**
     * Dump datasets statistics to a cache file.
     * <p>
     * The task runs on an independent thread, see private method {@code scheduleDatasetsStatsUpdate} in {@link CacheUpdater}.
     * Log anything that may be thrown to avoid a silent death if something goes wrong.
     */
    public static void dumpStatistics() {
        try {
            JSONObject statementsStats = fetchStatistics("statements");
            if (statementsStats == null) return;
            JSONObject referencesStats = fetchStatistics("references");
            if (referencesStats == null) return;
            for (Object k : statementsStats.keySet()) {
                String key = (String) k;
                JSONObject statementsValue = (JSONObject) statementsStats.get(key);
                JSONObject referencesValue = (JSONObject) referencesStats.get(key);
                if (referencesValue == null) referencesStats.put(key, statementsStats.get(key));
                else referencesValue.putAll(statementsValue);
            }
            try (BufferedWriter writer = Files.newBufferedWriter(Config.DATASETS_CACHE)) {
                referencesStats.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping datasets statistics to '" + Config.DATASETS_CACHE + "'.", ioe);
                return;
            }
        } catch (Throwable t) {
            log.error("Something went wrong while caching datasets statistics", t);
            return;
        }
        log.info("Successfully cached datasets statistics");

    }

    private static JSONObject fetchStatistics(String statementsOrReferences) {
        JSONObject stats = new JSONObject();
        String query = statementsOrReferences.equals("statements") ? SparqlQueries.STATEMENTS_COUNT_QUERY : SparqlQueries.REFERENCES_COUNT_QUERY;
        TupleQueryResult rawStats = Utils.runSparqlQuery(query);
        if (rawStats == null) return null;
        try {
            while (rawStats.hasNext()) {
                BindingSet result = rawStats.next();
                String graph = result.getValue("graph").stringValue();
                int count = Integer.parseInt(result.getValue("count").stringValue());
                String[] graphParts = graph.split("/");
                String key = graphParts[2];
                String state = graphParts[graphParts.length - 1];
                JSONObject currentStats = (JSONObject) stats.getOrDefault(key, new JSONObject());
                switch (state) {
                case "new":
                    currentStats.put("missing_" + statementsOrReferences, count);
                    break;
                case "approved":
                    currentStats.put("approved_" + statementsOrReferences, count);
                    break;
                case "rejected":
                    currentStats.put("rejected_" + statementsOrReferences, count);
                    break;
                case "duplicate":
                    currentStats.put("duplicate_" + statementsOrReferences, count);
                    break;
                case "blacklisted":
                    currentStats.put("blacklisted_" + statementsOrReferences, count);
                    break;
                }
                stats.put(key, currentStats);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches dataset statistics: '" + query + "'", qee);
            return null;
        }
        JSONObject finalStats = new JSONObject();
        for (Object key : stats.keySet()) {
            String k = (String) key;
            int total = 0;
            JSONObject v = (JSONObject) stats.get(k);
            total += (int) v.getOrDefault("missing_" + statementsOrReferences, 0);
            total += (int) v.getOrDefault("approved_" + statementsOrReferences, 0);
            total += (int) v.getOrDefault("rejected_" + statementsOrReferences, 0);
            total += (int) v.getOrDefault("duplicate_" + statementsOrReferences, 0);
            total += (int) v.getOrDefault("blacklisted_" + statementsOrReferences, 0);
            v.put("total_" + statementsOrReferences, total);
            finalStats.put("http://" + k + "/new", v);
        }
        return finalStats;
    }
}
