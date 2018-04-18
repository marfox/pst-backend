package org.wikidata.query.rdf.primarysources.common;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.io.Resources;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 11, 2017.
 */
@RunWith(RandomizedRunner.class)
public class EntitiesCacheIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static final String SECOND_TEST_DATASET_FILE_NAME = "pieter.ttl";
    private static final String THIRD_TEST_DATASET_FILE_NAME = "barbara_and_victoria.ttl";

    private static File firstDataset;
    private static File secondDataset;
    private static File thirdDataset;

    private static JSONObject firstDatasetSubjectsCache;
    private static JSONObject firstDatasetPropertiesCache;
    private static JSONObject firstDatasetValuesCache;
    private static JSONObject secondDatasetSubjectsCache;
    private static JSONObject secondDatasetPropertiesCache;
    private static JSONObject secondDatasetValuesCache;
    private static JSONObject thirdDatasetSubjectsCache;
    private static JSONObject thirdDatasetPropertiesCache;
    private static JSONObject thirdDatasetValuesCache;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        firstDataset = new File(Resources.getResource(CurationAPIIntegrationTest.TEST_DATASET_FILE_NAME).toURI());
        secondDataset = new File(Resources.getResource(SECOND_TEST_DATASET_FILE_NAME).toURI());
        thirdDataset = new File(Resources.getResource(THIRD_TEST_DATASET_FILE_NAME).toURI());
    }

    @AfterClass
    public static void deleteCache() throws IOException {
        Files.deleteIfExists(EntitiesCache.SUBJECTS_CACHE_FILE);
        Files.deleteIfExists(EntitiesCache.PROPERTIES_CACHE_FILE);
        Files.deleteIfExists(EntitiesCache.VALUES_CACHE_FILE);
    }

    private void purgeCache() throws Exception {
        purgeCacheFile(EntitiesCache.SUBJECTS_CACHE_FILE);
        purgeCacheFile(EntitiesCache.PROPERTIES_CACHE_FILE);
        purgeCacheFile(EntitiesCache.VALUES_CACHE_FILE);
    }

    private void purgeCacheFile(Path path) throws IOException {
        Files.deleteIfExists(path);
        JSONObject empty = new JSONObject();
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            empty.writeJSONString(writer);
        }
    }

    @Before
    public void prepareDatasetEntitiesCache() throws Exception {
        purgeCache();
        JSONParser parser = new JSONParser();
        uploadDataset("chuck berry", firstDataset);
        // The cache updater runs on a separate thread, so wait a bit after the upload
        sleep(500);
        firstDatasetSubjectsCache = parseCache(EntitiesCache.SUBJECTS_CACHE_FILE, parser);
        firstDatasetPropertiesCache = parseCache(EntitiesCache.PROPERTIES_CACHE_FILE, parser);
        firstDatasetValuesCache = parseCache(EntitiesCache.VALUES_CACHE_FILE, parser);
        uploadDataset("pieter", secondDataset);
        sleep(500);
        secondDatasetSubjectsCache = parseCache(EntitiesCache.SUBJECTS_CACHE_FILE, parser);
        secondDatasetPropertiesCache = parseCache(EntitiesCache.PROPERTIES_CACHE_FILE, parser);
        secondDatasetValuesCache = parseCache(EntitiesCache.VALUES_CACHE_FILE, parser);
        uploadDataset("b and v", thirdDataset);
        sleep(500);
        thirdDatasetSubjectsCache = parseCache(EntitiesCache.SUBJECTS_CACHE_FILE, parser);
        thirdDatasetPropertiesCache = parseCache(EntitiesCache.PROPERTIES_CACHE_FILE, parser);
        thirdDatasetValuesCache = parseCache(EntitiesCache.VALUES_CACHE_FILE, parser);
        purgeCache();
    }

    private JSONObject parseCache(Path path, JSONParser parser) throws IOException, ParseException {
        Object parsed;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            parsed = parser.parse(reader);
        }
        return (JSONObject) parsed;
    }

    private void uploadDataset(String datasetName, File dataset) throws IOException {
        MultipartEntityBuilder multipart = MultipartEntityBuilder.create();
        multipart.addTextBody("name", datasetName, ContentType.TEXT_PLAIN);
        multipart.addTextBody("user", "IMDataProvider", ContentType.TEXT_PLAIN);
        multipart.addBinaryBody("dataset", dataset);
        Request.Post(CurationAPIIntegrationTest.UPLOAD_ENDPOINT)
                .body(multipart.build())
                .execute()
                .discardContent();
    }

    @Test
    public void testCacheDatasetEntities() {
        /* Cache after first test dataset upload */
        // Subjects
        assertEquals(1, firstDatasetSubjectsCache.size());
        String firstDatasetUri = "http://chuck-berry/new";
        assertTrue(firstDatasetSubjectsCache.containsKey(firstDatasetUri));
        JSONArray subjects = (JSONArray) firstDatasetSubjectsCache.get(firstDatasetUri);
        assertEquals(1, subjects.size());
        assertEquals(CurationAPIIntegrationTest.TEST_QID, subjects.get(0));
        // Properties
        assertEquals(1, firstDatasetPropertiesCache.size());
        assertTrue(firstDatasetPropertiesCache.containsKey(firstDatasetUri));
        JSONArray properties = (JSONArray) firstDatasetPropertiesCache.get(firstDatasetUri);
        assertEquals(2, properties.size());
        assertTrue(properties.contains("P999"));
        // Values
        assertEquals(1, firstDatasetValuesCache.size());
        assertTrue(firstDatasetValuesCache.containsKey(firstDatasetUri));
        JSONArray values = (JSONArray) firstDatasetValuesCache.get(firstDatasetUri);
        assertEquals(2, values.size());
        assertTrue(values.contains("Q123456"));

        /* Cache after second test dataset upload */
        //Subjects
        assertEquals(2, secondDatasetSubjectsCache.size());
        String secondDatasetUri = "http://pieter/new";
        assertTrue(secondDatasetSubjectsCache.containsKey(firstDatasetUri));
        assertTrue(secondDatasetSubjectsCache.containsKey(secondDatasetUri));
        subjects = (JSONArray) secondDatasetSubjectsCache.get(secondDatasetUri);
        assertEquals(1, subjects.size());
        assertEquals("Q2094286", subjects.get(0));
        // Properties
        assertEquals(2, secondDatasetPropertiesCache.size());
        assertTrue(secondDatasetPropertiesCache.containsKey(firstDatasetUri));
        assertTrue(secondDatasetPropertiesCache.containsKey(secondDatasetUri));
        properties = (JSONArray) secondDatasetPropertiesCache.get(secondDatasetUri);
        assertEquals(8, properties.size());
        assertTrue(properties.contains("P569"));
        // Values
        assertEquals(2, secondDatasetValuesCache.size());
        assertTrue(secondDatasetValuesCache.containsKey(secondDatasetUri));
        values = (JSONArray) secondDatasetValuesCache.get(secondDatasetUri);
        assertEquals(3, values.size());
        assertTrue(values.contains("Q11569986"));

        /* Cache after third test dataset upload */
        // Subjects
        assertEquals(3, thirdDatasetSubjectsCache.size());
        String thirdDatasetUri = "http://b-and-v/new";
        assertTrue(thirdDatasetSubjectsCache.containsKey(firstDatasetUri));
        assertTrue(thirdDatasetSubjectsCache.containsKey(secondDatasetUri));
        assertTrue(thirdDatasetSubjectsCache.containsKey(thirdDatasetUri));
        subjects = (JSONArray) thirdDatasetSubjectsCache.get(thirdDatasetUri);
        assertEquals(2, subjects.size());
        assertTrue(subjects.contains("Q21462724"));
        assertTrue(subjects.contains("Q22672029"));
        // Properties
        assertEquals(3, thirdDatasetPropertiesCache.size());
        assertTrue(thirdDatasetPropertiesCache.containsKey(firstDatasetUri));
        assertTrue(thirdDatasetPropertiesCache.containsKey(secondDatasetUri));
        assertTrue(thirdDatasetPropertiesCache.containsKey(thirdDatasetUri));
        properties = (JSONArray) thirdDatasetPropertiesCache.get(thirdDatasetUri);
        assertEquals(3, properties.size());
        assertTrue(properties.contains("P1559"));
        // Values
        assertEquals(3, thirdDatasetValuesCache.size());
        assertTrue(thirdDatasetValuesCache.containsKey(firstDatasetUri));
        assertTrue(thirdDatasetValuesCache.containsKey(secondDatasetUri));
        assertTrue(thirdDatasetValuesCache.containsKey(thirdDatasetUri));
        values = (JSONArray) thirdDatasetValuesCache.get(thirdDatasetUri);
        assertEquals(1, values.size());
        assertEquals("Q6581072", values.get(0));
    }

    @Test
    public void testDumpAllEntities() throws Exception {
        JSONParser parser = new JSONParser();
        EntitiesCache.dumpAllEntities();
        // Subjects
        JSONObject currentSubjectsCache = parseCache(EntitiesCache.SUBJECTS_CACHE_FILE, parser);
        assertTrue(currentSubjectsCache.containsKey("http://chuck-berry/new"));
        assertTrue(currentSubjectsCache.containsKey("http://pieter/new"));
        assertTrue(currentSubjectsCache.containsKey("http://b-and-v/new"));
        JSONArray subjects = (JSONArray) currentSubjectsCache.get("http://b-and-v/new");
        assertEquals(2, subjects.size());
        assertTrue(subjects.contains("Q21462724"));
        assertTrue(subjects.contains("Q22672029"));
        // Properties
        JSONObject currentPropertiesCache = parseCache(EntitiesCache.PROPERTIES_CACHE_FILE, parser);
        assertTrue(currentPropertiesCache.containsKey("http://chuck-berry/new"));
        assertTrue(currentPropertiesCache.containsKey("http://pieter/new"));
        assertTrue(currentPropertiesCache.containsKey("http://b-and-v/new"));
        JSONArray properties = (JSONArray) currentPropertiesCache.get("http://pieter/new");
        assertEquals(8, properties.size());
        assertTrue(properties.contains("P21"));
        assertTrue(properties.contains("P742"));
        // Values
        JSONObject currentValuesCache = parseCache(EntitiesCache.VALUES_CACHE_FILE, parser);
        assertTrue(currentValuesCache.containsKey("http://chuck-berry/new"));
        assertTrue(currentValuesCache.containsKey("http://pieter/new"));
        assertTrue(currentValuesCache.containsKey("http://b-and-v/new"));
        JSONArray values = (JSONArray) currentValuesCache.get("http://pieter/new");
        assertEquals(3, values.size());
        assertTrue(values.contains("Q6581097"));
        assertTrue(values.contains("Q11569986"));
    }
}