package org.wikidata.query.rdf.primarysources.statistics;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.io.Resources;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.hamcrest.Matchers;
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
import static org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Jan 05, 2018.
 */
@RunWith(RandomizedRunner.class)
public class StatisticsAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static URI statisticsEndpoint;
    private static URI curateEndpoint;
    private static File testDataset;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        statisticsEndpoint = URI.create(BASE_ENDPOINT + "/statistics");
        curateEndpoint = URI.create(BASE_ENDPOINT + "/curate");
        testDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
    }

    @Before
    public void uploadTestDataset() throws Exception {
        CurationAPIIntegrationTest.uploadTestDataset(testDataset);
    }

    @AfterClass
    public static void deleteCache() throws Exception {
        Files.deleteIfExists(DATASETS_CACHE_PATH);
    }

    @Test
    public void testDatasetStatistics() throws Exception {
        DatasetsStatisticsCache.dumpStatistics();
        URIBuilder builder = new URIBuilder(statisticsEndpoint);
        JSONParser parser = new JSONParser();
        // Proper call
        JSONObject stats = testSuccess(builder, parser, "dataset", "http://chuck-berry/new");
        long totalStatements = (long) stats.get("total_statements");
        long totalReferences = (long) stats.get("total_references");
        assertEquals(totalStatements, 5);
        assertEquals(totalReferences, totalStatements);
        assertEquals(stats.get("missing_statements"), totalStatements);
        assertEquals(stats.get("missing_references"), totalStatements);
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
        curated.put("dataset", "http://chuck-berry/new");
        curated.put("state", "rejected");
        curated.put("user", "IMCurator");
        Request.Post(curateEndpoint)
                .bodyString(curated.toJSONString(), ContentType.APPLICATION_JSON)
                .execute()
                .discardContent();
        JSONObject stats = testSuccess(builder, parser, "user", "IMCurator");
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

    private JSONObject testSuccess(URIBuilder builder, JSONParser parser, String datasetOrUser, String value) throws Exception {
        builder.setParameter(datasetOrUser, value);
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