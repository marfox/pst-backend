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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static org.wikidata.query.rdf.primarysources.curation.RandomServlet.CACHED_SUBJECTS;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 07, 2017.
 */
public class SubjectsCache {

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

    // Run when the cache expires
    // TODO set the cache expiry and use this method elsewhere
    public static void updateCache() {
        ExecutorService service = ForkJoinPool.commonPool();
        service.submit(() -> dumpAllSubjects());
    }

    private static void dumpAllSubjects() {
        /*
         The task runs on an indpendent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            JSONObject subjects = fetchAllSubjects();
            if (subjects == null) return;
            try (BufferedWriter writer = Files.newBufferedWriter(CACHED_SUBJECTS)) {
                subjects.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping all the subjects to '" + CACHED_SUBJECTS + "'.", ioe);
            }
        } catch (Throwable t) {
            log.error("Something went wrong while caching all the subjects", t);
        }
    }

    private static JSONObject fetchAllSubjects() {
        JSONObject subjects = new JSONObject();
        TupleQueryResult results = runSparqlQuery(ALL_DATASETS_SUBJECT_LIST_QUERY);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String subject = result.getValue("subject").stringValue();
                String dataset = result.getValue("dataset").stringValue();
                Set<String> datasetSubjects = (Set<String>) subjects.getOrDefault(dataset, new HashSet<>());
                datasetSubjects.add(subject.substring(WIKIBASE_URIS.entity().length()));
                subjects.put(dataset, datasetSubjects);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the SPARQL query that fetches subject items: '" + ALL_DATASETS_SUBJECT_LIST_QUERY + "'", qee);
            return null;
        }
        return subjects;
    }

    private static void dumpDatasetSubjects(String dataset) {
        /*
         The task runs on an indpendent thread, so prevent it from dying quietly if something goes wrong.
         Log anything that may be thrown.
          */
        try {
            JSONParser parser = new JSONParser();
            Object parsed = null;
            try (BufferedReader reader = Files.newBufferedReader(CACHED_SUBJECTS)) {
                parsed = parser.parse(reader);
            } catch (ParseException pe) {
                log.error("Malformed JSON subject list. Parse error at index {}. Please check {}", pe.getPosition(), CACHED_SUBJECTS);
            } catch (IOException ioe) {
                log.error("Something went wrong while loading subjects from '" + CACHED_SUBJECTS + "'", ioe);
            }
            JSONObject oldSubjects = parsed == null ? new JSONObject() : (JSONObject) parsed;
            JSONObject newSubjects = fetchDatasetSubjects(dataset, oldSubjects);
            if (newSubjects == null) return;
            try (BufferedWriter writer = Files.newBufferedWriter(CACHED_SUBJECTS)) {
                // FIXME doesn't dump the array of subjects properly --> {"http:\/\/chuck-berry\/new":[Q5921]}
                newSubjects.writeJSONString(writer);
            } catch (IOException ioe) {
                log.error("Something went wrong when dumping subjects of dataset '" + dataset + "' to '" + CACHED_SUBJECTS + "'", ioe);
            }
        } catch (Throwable t) {
            log.error("Something went wrong while caching subjects of dataset " + dataset, t);
        }
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
        oldSubjects.put(dataset, subjectSet);
        return oldSubjects;
    }
}
