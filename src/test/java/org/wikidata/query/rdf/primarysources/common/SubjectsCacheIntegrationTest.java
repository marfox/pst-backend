package org.wikidata.query.rdf.primarysources.common;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.io.Resources;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.primarysources.AbstractRdfRepositoryIntegrationTestBase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;

import static org.wikidata.query.rdf.primarysources.common.SubjectsCache.CACHE_PATH;
import static org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 11, 2017.
 */
@RunWith(RandomizedRunner.class)
public class SubjectsCacheIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static final String SECOND_TEST_DATASET_FILE_NAME = "pieter.ttl";
    private static final String THIRD_TEST_DATASET_FILE_NAME = "barbara_and_victoria.ttl";

    private static File firstDataset;
    private static File secondDataset;
    private static File thirdDataset;

    private static JSONObject firstDatasetCache;
    private static JSONObject secondDatasetCache;
    private static JSONObject thirdDatasetCache;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        firstDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
        secondDataset = new File(Resources.getResource(SECOND_TEST_DATASET_FILE_NAME).toURI());
        thirdDataset = new File(Resources.getResource(THIRD_TEST_DATASET_FILE_NAME).toURI());
    }

    private void purgeCache() throws Exception {
        Files.deleteIfExists(CACHE_PATH);
        JSONObject empty = new JSONObject();
        try (BufferedWriter writer = Files.newBufferedWriter(CACHE_PATH)) {
            empty.writeJSONString(writer);
        }
    }

    @Before
    public void prepareDatasetSubjectsCache() throws Exception {
        purgeCache();
        JSONParser parser = new JSONParser();
        uploadDataset("chuck berry", firstDataset);
        // The cache updater runs on a separate thread, so wait a bit after the upload
        sleep(200);
        firstDatasetCache = parseCache(parser);
        uploadDataset("pieter", secondDataset);
        sleep(200);
        secondDatasetCache = parseCache(parser);
        uploadDataset("b and v", thirdDataset);
        sleep(200);
        thirdDatasetCache = parseCache(parser);
        purgeCache();
    }

    private JSONObject parseCache(JSONParser parser) throws IOException, ParseException {
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(CACHE_PATH)) {
            parsed = parser.parse(reader);
        }
        return (JSONObject) parsed;
    }

    private void uploadDataset(String datasetName, File dataset) throws IOException {
        MultipartEntityBuilder multipart = MultipartEntityBuilder.create();
        multipart.addTextBody("name", datasetName, ContentType.TEXT_PLAIN);
        multipart.addTextBody("user", "IMDataProvider", ContentType.TEXT_PLAIN);
        multipart.addBinaryBody("dataset", dataset);
        Request.Post(UPLOAD_ENDPOINT)
                .body(multipart.build())
                .execute()
                .discardContent();
    }

    @Test
    public void testCacheDatasetSubjects() throws Exception {
        // Cache after first test dataset upload
        assertEquals(1, firstDatasetCache.size());
        String firstDatasetUri = "http://chuck-berry/new";
        assertTrue(firstDatasetCache.containsKey(firstDatasetUri));
        JSONArray subjects = (JSONArray) firstDatasetCache.get(firstDatasetUri);
        assertEquals(1, subjects.size());
        assertEquals(TEST_QID, subjects.get(0));
        // Cache after second test dataset upload
        assertEquals(2, secondDatasetCache.size());
        String secondDatasetUri = "http://pieter/new";
        assertTrue(secondDatasetCache.containsKey(firstDatasetUri));
        assertTrue(secondDatasetCache.containsKey(secondDatasetUri));
        subjects = (JSONArray) secondDatasetCache.get(secondDatasetUri);
        assertEquals(1, subjects.size());
        assertEquals("Q2094286", subjects.get(0));
        // Cache after third test dataset upload
        assertEquals(3, thirdDatasetCache.size());
        String thirdDatasetUri = "http://b-and-v/new";
        assertTrue(thirdDatasetCache.containsKey(firstDatasetUri));
        assertTrue(thirdDatasetCache.containsKey(secondDatasetUri));
        assertTrue(thirdDatasetCache.containsKey(thirdDatasetUri));
        subjects = (JSONArray) thirdDatasetCache.get(thirdDatasetUri);
        assertEquals(2, subjects.size());
        assertTrue(subjects.contains("Q21462724"));
        assertTrue(subjects.contains("Q22672029"));
    }

    /*
     This test won't work because Sesame complains for a null value, when SubjectsCache.dumpAllSubjects runs a SPARQL query.
     Might stem from missing dependencies at test scope.
     org.openrdf.query.resultio.UnsupportedQueryResultFormatException: No parser factory available for tuple query result format null
	   at org.openrdf.query.resultio.QueryResultIO.createParser(QueryResultIO.java:355)
	   at org.openrdf.query.resultio.QueryResultIO.parse(QueryResultIO.java:552)
	   at org.wikidata.query.rdf.primarysources.curation.SuggestServlet.runSparqlQuery(SuggestServlet.java:159)
	   at org.wikidata.query.rdf.primarysources.common.SubjectsCache.fetchAllSubjects(SubjectsCache.java:85)
      */
    @Test
    @Ignore
    public void testDumpAllSubjects() throws Exception {
        JSONParser parser = new JSONParser();
        SubjectsCache.dumpAllSubjects();
        JSONObject currentCache = parseCache(parser);
        assertTrue(currentCache.containsKey("http://chuck-berry/new"));
        assertTrue(currentCache.containsKey("http://pieter/new"));
        assertTrue(currentCache.containsKey("http://b-and-v/new"));
        JSONArray subjects = (JSONArray) currentCache.get("http://b-and-v/new");
        assertEquals(2, subjects.size());
        assertTrue(subjects.contains("Q21462724"));
        assertTrue(subjects.contains("Q22672029"));
    }
}