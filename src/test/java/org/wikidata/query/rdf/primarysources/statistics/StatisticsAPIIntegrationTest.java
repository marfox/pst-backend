package org.wikidata.query.rdf.primarysources.statistics;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.io.Resources;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.hamcrest.Matchers;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.*;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.primarysources.AbstractRdfRepositoryIntegrationTestBase;
import org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;

import static org.wikidata.query.rdf.primarysources.common.DatasetsStatisticsCache.DATASETS_CACHE_PATH;
import static org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest.BASE_ENDPOINT;
import static org.wikidata.query.rdf.primarysources.curation.CurationAPIIntegrationTest.TEST_DATASET_FILE_NAME;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Jan 05, 2018.
 */
@RunWith(RandomizedRunner.class)
public class StatisticsAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static URI statisticsEndpoint;
    private static File testDataset;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        statisticsEndpoint = URI.create(BASE_ENDPOINT + "/statistics");
        testDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
    }

    @AfterClass
    public static void deleteCache() throws Exception {
        Files.deleteIfExists(DATASETS_CACHE_PATH);
    }

    @Before
    public void setUp() throws Exception {
        CurationAPIIntegrationTest.uploadTestDataset(testDataset);
    }

    @Test
    public void testDatasetStatistics() throws Exception {
        URIBuilder builder = new URIBuilder(statisticsEndpoint);
        JSONParser parser = new JSONParser();
        // Success
        builder.setParameter("dataset", "http://chuck-berry/new");
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONObject.class));
        JSONObject stats = (JSONObject) parsed;
        long totalStatements = (long) stats.get("total_statements");
        long totalReferences = (long) stats.get("total_references");
        assertEquals(totalStatements, 5);
        assertEquals(totalReferences, totalStatements);
        assertEquals(stats.get("missing_statements"), totalStatements);
        assertEquals(stats.get("missing_references"), totalStatements);
        // Failure
        builder.setParameter("dataset", "");
        HttpResponse response = Request.Get(builder.build())
                .execute()
                .returnResponse();
        assertEquals(400, response.getStatusLine().getStatusCode());
    }

}