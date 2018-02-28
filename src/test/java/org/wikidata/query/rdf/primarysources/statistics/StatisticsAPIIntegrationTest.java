package org.wikidata.query.rdf.primarysources.statistics;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.io.Resources;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.hamcrest.Matchers;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.*;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.primarysources.AbstractRdfRepositoryIntegrationTestBase;
import org.wikidata.query.rdf.primarysources.common.DatasetsStatisticsCache;
import org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

import static org.wikidata.query.rdf.primarysources.common.DatasetsStatisticsCache.DATASETS_CACHE_PATH;
import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.PROPERTIES_CACHE_FILE;
import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.SUBJECTS_CACHE_FILE;
import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.VALUES_CACHE_FILE;
import static org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest.*;
import static org.wikidata.query.rdf.primarysources.ingestion.IngestionAPIIntegrationTest.*;
import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.USER_URI_PREFIX;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Jan 05, 2018.
 */
@RunWith(RandomizedRunner.class)
public class StatisticsAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static URI statisticsEndpoint;
    private static URI curateEndpoint;
    private static URI propertiesEndpoint;
    private static URI valuesEndpoint;
    private static File testDataset;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        statisticsEndpoint = URI.create(BASE_ENDPOINT + "/statistics");
        curateEndpoint = URI.create(BASE_ENDPOINT + "/curate");
        propertiesEndpoint = URI.create(BASE_ENDPOINT + "/properties");
        valuesEndpoint = URI.create(BASE_ENDPOINT + "/values");
        testDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
    }

    @Before
    public void uploadTestDataset() throws Exception {
        CurationAPIIntegrationTest.uploadTestDataset(testDataset);
    }

    @AfterClass
    public static void deleteCache() throws Exception {
        Files.deleteIfExists(DATASETS_CACHE_PATH);
        Files.deleteIfExists(SUBJECTS_CACHE_FILE);
        Files.deleteIfExists(PROPERTIES_CACHE_FILE);
        Files.deleteIfExists(VALUES_CACHE_FILE);
    }

    @Test
    public void testProperties() throws Exception {
        testPropertiesOrValues(propertiesEndpoint, 2, "P18");
    }

    @Test
    public void testValues() throws Exception {
        testPropertiesOrValues(valuesEndpoint, 2, "Q123456");
    }

    private void testPropertiesOrValues(URI endpoint, int expectedListSize, String expectedEntity) throws Exception {
        URIBuilder builder = new URIBuilder(endpoint);
        JSONParser parser = new JSONParser();
        // Proper call
        JSONObject jsonResponse = testCorrectCall(builder, parser, "dataset", EXPECTED_DATASET_URI);
        JSONArray entities = (JSONArray) jsonResponse.get(EXPECTED_DATASET_URI);
        assertEquals(expectedListSize, entities.size());
        assertTrue(entities.contains(expectedEntity));
        // Bad call
        HttpResponse httpResponse = testClientError(builder, "rock'n'roll", "will save us");
        assertEquals(400, httpResponse.getStatusLine().getStatusCode());
        httpResponse = testClientError(builder, "dataset", "this is not a uri");
        assertEquals(400, httpResponse.getStatusLine().getStatusCode());
    }

    @Test
    public void testDatasetStatistics() throws Exception {
        DatasetsStatisticsCache.dumpStatistics();
        URIBuilder builder = new URIBuilder(statisticsEndpoint);
        JSONParser parser = new JSONParser();
        // Proper call
        JSONObject stats = testCorrectCall(builder, parser, "dataset", EXPECTED_DATASET_URI);
        long totalStatements = (long) stats.get("total_statements");
        long totalReferences = (long) stats.get("total_references");
        assertEquals(totalStatements, 5);
        assertEquals(totalReferences, totalStatements);
        assertEquals(stats.get("missing_statements"), totalStatements);
        assertEquals(stats.get("missing_references"), totalStatements);
        assertEquals(DATASET_DESCRIPTION, stats.get("description"));
        assertEquals(USER_URI_PREFIX + UPLOADER_NAME, stats.get("uploader"));
        // Bad call
        HttpResponse response = testClientError(builder, "dataset", "bad uri");
        assertEquals(400, response.getStatusLine().getStatusCode());
    }

    @Test
    public void testUserStatistics() throws Exception {
        URIBuilder builder = new URIBuilder(statisticsEndpoint);
        JSONParser parser = new JSONParser();
        // Success
        JSONObject curated = new JSONObject();
        curated.put("qs", TEST_QID + "\tP999\t\"Maybelline\"");
        curated.put("type", "claim");
        curated.put("dataset", EXPECTED_DATASET_URI);
        curated.put("state", "rejected");
        curated.put("user", "IMCurator");
        Request.Post(curateEndpoint)
                .bodyString(curated.toJSONString(), ContentType.APPLICATION_JSON)
                .execute()
                .discardContent();
        JSONObject stats = testCorrectCall(builder, parser, "user", "IMCurator");
        long activities = (long) stats.get("activities");
        assertEquals(1, activities);
        // Bad user name
        // non va
        HttpResponse response = testClientError(builder, "user", ":/?#[]@!$&'()*+,;=");
        assertEquals(400, response.getStatusLine().getStatusCode());
        // No user activity
        response = testClientError(builder, "user", "IMNoCurator");
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    private JSONObject testCorrectCall(URIBuilder builder, JSONParser parser, String parameter, String value) throws Exception {
        builder.setParameter(parameter, value);
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONObject.class));
        return (JSONObject) parsed;
    }

    private HttpResponse testClientError(URIBuilder builder, String datasetOrUser, String value) throws Exception {
        builder.setParameter(datasetOrUser, value);
        return Request.Get(builder.build())
                .execute()
                .returnResponse();
    }
}