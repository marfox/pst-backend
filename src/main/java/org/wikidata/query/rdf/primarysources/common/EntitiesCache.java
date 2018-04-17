package org.wikidata.query.rdf.primarysources.common;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.RDF;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 07, 2017.
 */
public class EntitiesCache {

    public static final Path SUBJECTS_CACHE_FILE = Paths.get(Config.ENTITIES_CACHE_DIR, "subjects.json");
    public static final Path PROPERTIES_CACHE_FILE = Paths.get(Config.ENTITIES_CACHE_DIR, "properties.json");
    public static final Path VALUES_CACHE_FILE = Paths.get(Config.ENTITIES_CACHE_DIR, "values.json");

    // A single query for subjects, properties, and values is too heavy, so split into 3
    private static final String SUBJECTS_ONE_DATASET_QUERY =
            "SELECT ?subject " + // No need for a DISTINCT here, one dataset should not have duplicate subjects
                    "WHERE {" +
                    "  GRAPH <" + SparqlQueries.DATASET_PLACE_HOLDER + "> {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?statement_node ." +
                    "  }" +
                    "}";
    // Only consider main PIDs, not qualifiers or references
    private static final String PROPERTIES_ONE_DATASET_QUERY =
            "SELECT DISTINCT ?property " +
                    "WHERE {" +
                    "  GRAPH <" + SparqlQueries.DATASET_PLACE_HOLDER + "> {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?statement_node ." +
                    "  }" +
                    "}";
    // Also include qualifier values
    private static final String VALUES_ONE_DATASET_QUERY =
            "SELECT DISTINCT ?value " +
                    "WHERE {" +
                    "  GRAPH <" + SparqlQueries.DATASET_PLACE_HOLDER + "> {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?st_node ." +
                    "    ?st_node ?st_property ?value ." +
                    "  }" +
                    "  FILTER STRSTARTS(str(?value), \"" + Utils.WIKIBASE_URIS.entity() + "Q\") ." +
                    "}";
    private static final String SUBJECTS_ALL_DATASETS_QUERY =
            "SELECT DISTINCT ?subject ?dataset " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?statement_node ." +
                    "  }" +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";
    private static final String PROPERTIES_ALL_DATASETS_QUERY =
            "SELECT DISTINCT ?property ?dataset " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?statement_node ." +
                    "  }" +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";
    private static final String VALUES_ALL_DATASETS_QUERY =
            "SELECT DISTINCT ?value ?dataset " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?subject a wikibase:Item ;" +
                    "      ?property ?st_node ." +
                    "    ?st_node ?st_property ?value ." +
                    "  }" +
                    "  FILTER STRSTARTS(str(?value), \"" + Utils.WIKIBASE_URIS.entity() + "Q\") ." +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";

    private static final Logger log = LoggerFactory.getLogger(EntitiesCache.class);

    // Run when a change to a dataset is made through the ingestion API
    public static void cacheDatasetEntities(String dataset) {
        ExecutorService service = ForkJoinPool.commonPool();
        service.submit(() -> dumpDatasetEntities(dataset));
    }

    public static void dumpAllEntities() {
        /*
         The task runs on an independent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            String subject = "subject";
            String property = "property";
            String value = "value";
            JSONObject subjects = fetchAllEntitiesPerType(subject);
            if (subjects != null) dumpAllEntitiesPerType(subject, subjects);
            JSONObject properties = fetchAllEntitiesPerType(property);
            if (properties != null) dumpAllEntitiesPerType(property, properties);
            JSONObject values = fetchAllEntitiesPerType(value);
            if (values != null) dumpAllEntitiesPerType(value, values);
        } catch (Throwable t) {
            log.error("Something went wrong while caching all the entities", t);
            return;
        }
        log.info("Successfully cached all the entities in the database");
    }

    private static void dumpAllEntitiesPerType(String entityType, JSONObject entities) {
        Path cache = getCachePath(entityType);
        if (cache == null) return;
        log.info("Caching {} entities. File: '{}'", entityType, cache);
        try (BufferedWriter writer = Files.newBufferedWriter(cache)) {
            entities.writeJSONString(writer);
        } catch (IOException ioe) {
            log.error("Something went wrong when dumping all " + entityType + " entities to '" + Config.ENTITIES_CACHE_DIR + "'.", ioe);
        }
    }

    private static JSONObject fetchAllEntitiesPerType(String entityType) {
        String query;
        int namespaceIndex;
        switch (entityType) {
            case "subject":
                query = SUBJECTS_ALL_DATASETS_QUERY;
                namespaceIndex = Utils.WIKIBASE_URIS.entity().length();
                break;
            case "property":
                query = PROPERTIES_ALL_DATASETS_QUERY;
                namespaceIndex = Utils.WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length();
                break;
            case "value":
                query = VALUES_ALL_DATASETS_QUERY;
                namespaceIndex = Utils.WIKIBASE_URIS.entity().length();
                break;
            default:
                log.error("Unexpected entity type '{}'. The cache for those entities will not be available", entityType);
                return null;
        }
        JSONObject entitiesJson = new JSONObject();
        Map<String, Set<String>> entitiesMap = new HashMap<>();
        TupleQueryResult results = Utils.runSparqlQuery(query);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String entity = result.getValue(entityType).stringValue();
                if (entity.startsWith(RDF.TYPE)) continue;
                String dataset = result.getValue("dataset").stringValue();
                Set<String> datasetEntities = entitiesMap.getOrDefault(dataset, new HashSet<>());
                datasetEntities.add(entity.substring(namespaceIndex));
                entitiesMap.put(dataset, datasetEntities);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches " + entityType + " items. " +
                    "The corresponding cache will not be available. Query: '" + query + "'", qee);
            return null;
        }
        for (String dataset : entitiesMap.keySet()) {
            // Lists are needed to properly serialize the JSON (invalid JSON with Sets)
            entitiesJson.put(dataset, new ArrayList<>(entitiesMap.get(dataset)));
        }
        return entitiesJson;
    }

    private static void dumpDatasetEntities(String dataset) {
        /*
         The task runs on an indpendent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            JSONParser parser = new JSONParser();
            dumpDatasetEntitiesPerType("subject", dataset, parser);
            dumpDatasetEntitiesPerType("property", dataset, parser);
            dumpDatasetEntitiesPerType("value", dataset, parser);
        } catch (Throwable t) {
            log.error("Something went wrong while caching entities of dataset " + dataset, t);
            return;
        }
        log.info("Successfully cached the entities of dataset <{}>", dataset);
    }

    private static void dumpDatasetEntitiesPerType(String entityType, String dataset, JSONParser parser) {
        Path cache = getCachePath(entityType);
        if (cache == null) return;
        log.info("Caching {} entities. File: '{}'", entityType, cache);
        Object parsed = null;
        try (BufferedReader reader = Files.newBufferedReader(cache)) {
            parsed = parser.parse(reader);
        } catch (ParseException pe) {
            log.warn("The {} cache is malformed JSON. Parse error at index {}. Will overwrite '{}'", entityType, pe.getPosition(), cache);
        } catch (IOException ioe) {
            log.warn("Something went wrong while loading the " + entityType + " cache. Will overwrite '" + cache + "'", ioe);
        }
        JSONObject oldEntities = parsed == null ? new JSONObject() : (JSONObject) parsed;
        JSONObject newEntities = fetchDatasetEntities(entityType, dataset, oldEntities);
        if (newEntities == null) return;
        try (BufferedWriter writer = Files.newBufferedWriter(cache)) {
            newEntities.writeJSONString(writer);
        } catch (IOException ioe) {
            log.error("Something went wrong when dumping " + entityType + " of dataset <" + dataset + "> to '" + cache + "'", ioe);
        }
    }

    private static Path getCachePath(String entityType) {
        Path cache;
        switch (entityType) {
            case "subject":
                cache = SUBJECTS_CACHE_FILE;
                break;
            case "property":
                cache = PROPERTIES_CACHE_FILE;
                break;
            case "value":
                cache = VALUES_CACHE_FILE;
                break;
            default:
                log.error("Invalid entity type '{}'. Expected one of 'subject', 'property' or 'value'. " +
                        "The cache for those entities will not be available", entityType);
                return null;
        }
        return cache;
    }

    private static JSONObject fetchDatasetEntities(String entityType, String dataset, JSONObject oldEntities) {
        Set<String> entitySet = new HashSet<>();
        String query;
        int namespaceIndex;
        switch (entityType) {
            case "subject":
                query = SUBJECTS_ONE_DATASET_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, dataset);
                namespaceIndex = Utils.WIKIBASE_URIS.entity().length();
                break;
            case "property":
                query = PROPERTIES_ONE_DATASET_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, dataset);
                namespaceIndex = Utils.WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length();
                break;
            case "value":
                query = VALUES_ONE_DATASET_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, dataset);
                namespaceIndex = Utils.WIKIBASE_URIS.entity().length();
                break;
            default:
                log.error("Invalid entity type '{}'. Expected one of 'subject', 'property' or 'value'. " +
                        "The cache for those entities will not be available", entityType);
                return null;
        }
        TupleQueryResult results = Utils.runSparqlQuery(query);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String entity = result.getValue(entityType).stringValue();
                if (entity.startsWith(RDF.TYPE)) continue;
                entitySet.add(entity.substring(namespaceIndex));
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches " + entityType + " items: '" + query + "'", qee);
            return null;
        }
        oldEntities.put(dataset, new ArrayList<>(entitySet));
        return oldEntities;
    }

}
