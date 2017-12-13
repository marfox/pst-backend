package org.wikidata.query.rdf.primarysources.common;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 07, 2017.
 */
public class SubjectsCache {

    /*
     An environment variable is needed for tests. A system property would not be read when testing.
     IMPORTANT: SUBJECTS_CACHE should always be exported, otherwise integration tests can't run:
     when Jetty is fired up, the listener SubjectsCacheUpdater looks for that variable.
      */
    public static final Path CACHE_PATH = Paths.get(System.getenv("SUBJECTS_CACHE"));

    private static final String ONE_DATASET_SUBJECT_LIST_QUERY =
            "SELECT ?subject " +
                    "WHERE {" +
                    "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
                    "    ?subject ?property ?value ." +
                    "    FILTER STRSTARTS(str(?subject), \"http://www.wikidata.org/entity/Q\") ." +
                    "  }" +
                    "}";
    private static final String ALL_DATASETS_SUBJECT_LIST_QUERY =
            "SELECT ?subject ?dataset " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?subject ?property ?value ." +
                    "    FILTER STRSTARTS(str(?subject), \"http://www.wikidata.org/entity/Q\") ." +
                    "  }" +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";

    private static final Logger log = LoggerFactory.getLogger(SubjectsCache.class);

    // Run when a change to a dataset is made through the ingestion API
    public static void cacheDatasetSubjects(String dataset) {
        ExecutorService service = ForkJoinPool.commonPool();
        service.submit(() -> dumpDatasetSubjects(dataset));
    }

    static void dumpAllSubjects() {
        /*
         The task runs on an indpendent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            JSONObject subjects = fetchAllSubjects();
            if (subjects == null) return;
            try (BufferedWriter writer = Files.newBufferedWriter(CACHE_PATH)) {
                subjects.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping all the subjects to '" + CACHE_PATH + "'.", ioe);
                return;
            }
        } catch (Throwable t) {
            log.error("Something went wrong while caching all the subjects", t);
            return;
        }
        log.info("Successfully cached all the subjects in the database");
    }

    private static JSONObject fetchAllSubjects() {
        JSONObject subjectsJson = new JSONObject();
        Map<String, Set<String>> subjectsMap = new HashMap<>();
        TupleQueryResult results = runSparqlQuery(ALL_DATASETS_SUBJECT_LIST_QUERY);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String subject = result.getValue("subject").stringValue();
                String dataset = result.getValue("dataset").stringValue();
                Set<String> datasetSubjects = subjectsMap.getOrDefault(dataset, new HashSet<>());
                datasetSubjects.add(subject.substring(WIKIBASE_URIS.entity().length()));
                subjectsMap.put(dataset, datasetSubjects);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches subject items: '" + ALL_DATASETS_SUBJECT_LIST_QUERY + "'", qee);
            return null;
        }
        for (String dataset : subjectsMap.keySet()) {
            // Lists are needed to properly serialize the JSON (invalid JSON with Sets)
            subjectsJson.put(dataset, new ArrayList<>(subjectsMap.get(dataset)));
        }
        return subjectsJson;
    }

    private static void dumpDatasetSubjects(String dataset) {
        /*
         The task runs on an indpendent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            JSONParser parser = new JSONParser();
            Object parsed = null;
            try (BufferedReader reader = Files.newBufferedReader(CACHE_PATH)) {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("The subjects cache is malformed JSON. Parse error at index {}. Will overwrite '{}'", pe.getPosition(), CACHE_PATH);
            } catch (IOException ioe) {
                log.error("Something went wrong while loading the subjects cache. Will overwrite '" + CACHE_PATH + "'", ioe);
            }
            JSONObject oldSubjects = parsed == null ? new JSONObject() : (JSONObject) parsed;
            JSONObject newSubjects = fetchDatasetSubjects(dataset, oldSubjects);
            if (newSubjects == null) return;
            try (BufferedWriter writer = Files.newBufferedWriter(CACHE_PATH)) {
                newSubjects.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping subjects of dataset '" + dataset + "' to '" + CACHE_PATH + "'", ioe);
                return;
            }
        } catch (Throwable t) {
            log.error("Something went wrong while caching subjects of dataset " + dataset, t);
            return;
        }
        log.info("Successfully cached the subjects of dataset <{}>", dataset);
    }

    private static JSONObject fetchDatasetSubjects(String dataset, JSONObject oldSubjects) {
        Set<String> subjectSet = new HashSet<>();
        String query = ONE_DATASET_SUBJECT_LIST_QUERY.replace(DATASET_PLACE_HOLDER, dataset);
        TupleQueryResult results = runSparqlQuery(query);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String subject = result.getValue("subject").stringValue();
                subjectSet.add(subject.substring(WIKIBASE_URIS.entity().length()));
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches subject items: '" + query + "'", qee);
            return null;
        }
        oldSubjects.put(dataset, new ArrayList<>(subjectSet));
        return oldSubjects;
    }
}
