package org.wikidata.query.rdf.primarysources.common;

import org.json.simple.JSONObject;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 20, 2017.
 */
public class DatasetsStatisticsCache {

    private static final Logger log = LoggerFactory.getLogger(DatasetsStatisticsCache.class);

    public static void dumpStatistics() {
        /*
         The task runs on an independent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
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
            try (BufferedWriter writer = Files.newBufferedWriter(Config.DATASETS_CACHE_PATH)) {
                referencesStats.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping datasets statistics to '" + Config.DATASETS_CACHE_PATH + "'.", ioe);
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
