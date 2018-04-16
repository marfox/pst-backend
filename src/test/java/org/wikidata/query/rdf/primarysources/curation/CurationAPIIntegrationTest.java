package org.wikidata.query.rdf.primarysources.curation;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.google.common.io.Resources;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.hamcrest.Matchers;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.*;
import org.junit.runner.RunWith;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.primarysources.AbstractRdfRepositoryIntegrationTestBase;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.wikidata.query.rdf.primarysources.common.EntitiesCache.*;
import static org.wikidata.query.rdf.primarysources.ingestion.IngestionAPIIntegrationTest.*;
import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Oct 10, 2017.
 */
@RunWith(RandomizedRunner.class)
public class CurationAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    public static final String BASE_ENDPOINT = "http://localhost:9999/bigdata";
    public static final URI UPLOAD_ENDPOINT = URI.create(BASE_ENDPOINT + "/upload");
    public static final String TEST_DATASET_FILE_NAME = "chuck_berry_for_curation.ttl";
    public static final String TEST_QID = "Q5921";

    private static URI suggestEndpoint;
    private static URI curateEndpoint;
    private static URI searchEndpoint;
    private static URI randomEndpoint;
    private static URI datasetsEndpoint;
    private static File testDataset;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        suggestEndpoint = URI.create(BASE_ENDPOINT + "/suggest");
        curateEndpoint = URI.create(BASE_ENDPOINT + "/curate");
        searchEndpoint = URI.create(BASE_ENDPOINT + "/search");
        randomEndpoint = URI.create(BASE_ENDPOINT + "/random");
        datasetsEndpoint = URI.create(BASE_ENDPOINT + "/datasets");
        testDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
        EntitiesCache.dumpAllEntities();
    }

    @AfterClass
    public static void deleteCache() throws IOException {
        Files.deleteIfExists(SUBJECTS_CACHE_FILE);
        Files.deleteIfExists(PROPERTIES_CACHE_FILE);
        Files.deleteIfExists(VALUES_CACHE_FILE);
    }

    public static void uploadTestDataset(File testDataset) throws IOException {
        MultipartEntityBuilder multipart = MultipartEntityBuilder.create();
        ContentType text = ContentType.TEXT_PLAIN;
        multipart.addTextBody(DATASET_NAME_FORM_FIELD, DATASET_NAME, text);
        multipart.addTextBody(DATASET_DESCRIPTION_FORM_FIELD, DATASET_DESCRIPTION, text);
        multipart.addTextBody(USER_NAME_FORM_FIELD, UPLOADER_NAME, text);
        multipart.addBinaryBody(FILE_FIELD, testDataset);
        Request.Post(UPLOAD_ENDPOINT)
                .body(multipart.build())
                .execute()
                .discardContent();
    }

    @Before
    public void setUp() throws Exception {
        uploadTestDataset(testDataset);
    }

    @Test
    public void testDatasets() throws Exception {
        URIBuilder builder = new URIBuilder(datasetsEndpoint);
        JSONParser parser = new JSONParser();
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray datasets = (JSONArray) parsed;
        JSONObject testDatasetAndUser = (JSONObject) datasets.get(0);
        assertEquals(testDatasetAndUser.get("dataset"), "http://chuck-berry/new");
        assertEquals(testDatasetAndUser.get("user"), "http://www.wikidata.org/wiki/User:IMDataProvider");
    }

    @Test
    public void testRandom() throws Exception {
        URIBuilder builder = new URIBuilder(randomEndpoint);
        JSONParser parser = new JSONParser();
        // Default behavior
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        // The test dataset has 1 subject item with 5 references
        assertEquals(5, suggestions.size());
        // Dataset parameter
        builder.setParameter("dataset", "http://chuck-berry/new");
        responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        suggestions = (JSONArray) parsed;
        assertEquals(5, suggestions.size());
    }

    @Test
    public void testSuggest() throws Exception {
        URIBuilder builder = new URIBuilder(suggestEndpoint);
        // Success
        builder.setParameter("qid", TEST_QID);
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        JSONParser parser = new JSONParser();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        assertEquals(5, suggestions.size());
        TupleQueryResult statements = rdfRepository().query("select (count(?s) as ?count) where { graph <http://chuck-berry/new> { ?x ?p ?s . filter contains(str(?p), \"statement/\") . } }");
        TupleQueryResult qualifiers = rdfRepository().query("select (count(?q) as ?count) where { graph <http://chuck-berry/new> { ?x ?p ?q . filter contains(str(?p), \"qualifier/\") . } }");
        TupleQueryResult references = rdfRepository().query("select (count(?r) as ?count) where { graph <http://chuck-berry/new> { ?x prov:wasDerivedFrom ?r . } }");
        int expectedStatements = Integer.valueOf(statements.next().getValue("count").stringValue());
        int expectedQualifers = Integer.valueOf(qualifiers.next().getValue("count").stringValue());
        int expectedReferences = Integer.valueOf(references.next().getValue("count").stringValue());
        int actualStatements = 0;
        int actualQualifiers = 0;
        int actualReferences = 0;
        for (Object s : suggestions) {
            actualStatements += 1;
            JSONObject suggestion = (JSONObject) s;
            String quickStatement = (String) suggestion.get("statement");
            List<String> parts = Arrays.asList(quickStatement.split("\t"));
            List<String> qualifierAndReferences = parts.subList(3, parts.size());
            for (String part : qualifierAndReferences) {
                if (part.matches("^P\\d+$")) actualQualifiers += 1;
                else if (part.matches("^S\\d+$")) actualReferences += 1;
            }
        }
        assertEquals(expectedStatements, actualStatements);
        assertEquals(expectedQualifers, actualQualifiers);
        assertEquals(expectedReferences, actualReferences);
        // Failure
        builder.setParameter("qid", "Q666");
        int status = Request.Get(builder.build())
                .execute()
                .returnResponse()
                .getStatusLine()
                .getStatusCode();
        assertEquals(404, status);
    }

    @Test
    public void testSearch() throws Exception {
        URIBuilder builder = new URIBuilder(searchEndpoint);
        JSONParser parser = new JSONParser();
        testSearchDefaultBehavior(builder, parser);
        // Offset beyond the dataset size
        testSearchFailure(builder);
        testSearchWithLimit(builder, parser);
        testSearchWithOffset(builder, parser);
        testSearchWithProperty(builder, parser);
        testSearchWithValue(builder);
    }

    private void testSearchWithValue(URIBuilder builder) throws Exception {
        builder.clearParameters();
        builder.setParameter("value", "Q666");
        HttpResponse response = Request.Get(builder.build())
                .execute()
                .returnResponse();
        // No statement with value Q666 exists in the test dataset
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    private void testSearchWithProperty(URIBuilder builder, JSONParser parser) throws Exception {
        builder.clearParameters();
        builder.setParameter("property", "P999");
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        assertEquals(2, suggestions.size());
    }

    private void testSearchFailure(URIBuilder builder) throws Exception {
        builder.clearParameters();
        builder.setParameter("offset", "20");
        HttpResponse response = Request.Get(builder.build())
                .execute()
                .returnResponse();
        assertEquals(404, response.getStatusLine().getStatusCode());
    }

    private void testSearchWithOffset(URIBuilder builder, JSONParser parser) throws Exception {
        builder.clearParameters();
        builder.setParameter("offset", "10");
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        assertEquals(5, suggestions.size());
    }

    private void testSearchDefaultBehavior(URIBuilder builder, JSONParser parser) throws Exception {
        builder.clearParameters();
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        // Default limit = 50, test dataset = 16 triples, 5 QuickStatements
        assertEquals(5, suggestions.size());
    }

    private void testSearchWithLimit(URIBuilder builder, JSONParser parser) throws Exception {
        builder.clearParameters();
        builder.setParameter("limit", "12");
        String responseContent = Request.Get(builder.build())
                .execute()
                .returnContent()
                .asString();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        assertEquals(5, suggestions.size());
    }

    @Test
    public void testApproveClaim() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qs", TEST_QID + "\tP999\t\"Maybelline\"");
        curated.put("type", "claim");
        curated.put("dataset", "http://chuck-berry/new");
        curated.put("state", "approved");
        curated.put("user", "IMCurator");
        Request.Post(curateEndpoint)
                .bodyString(curated.toJSONString(), ContentType.APPLICATION_JSON)
                .execute()
                .discardContent();
        TupleQueryResult approvedResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/approved> { ?s ?p ?o . } }");
        TupleQueryResult stillNewResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/new> { ?s ?p ?o . } }");
        TupleQueryResult totalResult = rdfRepository().query("select (count (?s) as ?count) where { graph ?g { ?s ?p ?o . filter contains(str(?g), \"chuck-berry\") . } }");
        int approved = Integer.valueOf(approvedResult.next().getValue("count").stringValue());
        int stillNew = Integer.valueOf(stillNewResult.next().getValue("count").stringValue());
        int total = Integer.valueOf(totalResult.next().getValue("count").stringValue());
        assertEquals(total - stillNew, approved);
        /* Approve statement and qualifiers, NOT references:
           1 statement (wds:xxx ps:P999 "Maybelline") + 3 qualifiers, exclude 1 reference node
           + 1 twin statement needed for another reference (wds:yyy ps:P999 "Maybelline") + 0 qualifiers, exclude 1 reference node
           Total approved = 5
         */
        assertEquals(5, approved);
        assertEquals(total - approved, stillNew);
        assertEquals(22, stillNew);
    }

    @Test
    public void testRejectClaim() throws Exception {
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
        TupleQueryResult rejectedResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/rejected> { ?s ?p ?o . } }");
        TupleQueryResult stillNewResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/new> { ?s ?p ?o . } }");
        TupleQueryResult totalResult = rdfRepository().query("select (count (?s) as ?count) where { graph ?g { ?s ?p ?o . filter contains(str(?g), \"chuck-berry\") . } }");
        int rejected = Integer.valueOf(rejectedResult.next().getValue("count").stringValue());
        int stillNew = Integer.valueOf(stillNewResult.next().getValue("count").stringValue());
        int total = Integer.valueOf(totalResult.next().getValue("count").stringValue());
        assertEquals(total - stillNew, rejected);
        /* Reject everything:
           1 main node (wd:Q5921 p:P999 wds:xxx) + 1 statement (wds:xxx ps:P999 "Maybelline") + 3 qualifiers + 1 reference node + 1 reference value
           + 1 twin main node (wd:Q5921 p:P999 wds:yyy) + 1 twin statement (wds:yyy ps:P999 "Maybelline") needed for another reference + 1 reference node + 1 reference value
           Total rejected = 11
         */
        assertEquals(11, rejected);
        assertEquals(total - rejected, stillNew);
        assertEquals(16, stillNew);
    }

    @Test
    public void testApproveReference() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qs", TEST_QID + "\tP18\t\"http://commons.wikimedia.org/wiki/Special:FilePath/Chuck-berry-2007-07-18.jpg\"\tS854\t\"https://travisraminproducer.bandcamp.com/\"");
        curated.put("type", "reference");
        curated.put("dataset", "http://chuck-berry/new");
        curated.put("state", "approved");
        curated.put("user", "IMCurator");
        Request.Post(curateEndpoint)
                .bodyString(curated.toJSONString(), ContentType.APPLICATION_JSON)
                .execute()
                .discardContent();
        TupleQueryResult approvedResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/approved> { ?s ?p ?o . } }");
        TupleQueryResult stillNewResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/new> { ?s ?p ?o . } }");
        TupleQueryResult totalResult = rdfRepository().query("select (count (?s) as ?count) where { graph ?g { ?s ?p ?o . filter contains(str(?g), \"chuck-berry\") . } }");
        int approved = Integer.valueOf(approvedResult.next().getValue("count").stringValue());
        int stillNew = Integer.valueOf(stillNewResult.next().getValue("count").stringValue());
        int total = Integer.valueOf(totalResult.next().getValue("count").stringValue());
        assertEquals(total - stillNew, approved);
        /* Approve everything:
           1 main node (wd:Q5921 p:P18 wds:xxx) + 1 statement (wds:xxx ps:P18 URL) + 1 qualifier + 1 reference node + reference value
           Total approved = 5
         */
        assertEquals(5, approved);
        assertEquals(total - approved, stillNew);
        assertEquals(22, stillNew);
    }

    @Test
    public void testRejectReference() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qs", TEST_QID + "\tP18\t\"http://commons.wikimedia.org/wiki/Special:FilePath/Chuck-berry-2007-07-18.jpg\"\tS854\t\"https://travisraminproducer.bandcamp.com/\"");
        curated.put("type", "reference");
        curated.put("dataset", "http://chuck-berry/new");
        curated.put("state", "rejected");
        curated.put("user", "IMCurator");
        Request.Post(curateEndpoint)
                .bodyString(curated.toJSONString(), ContentType.APPLICATION_JSON)
                .execute()
                .discardContent();
        TupleQueryResult rejectedResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/rejected> { ?s ?p ?o . } }");
        TupleQueryResult stillNewResult = rdfRepository().query("select (count (?s) as ?count) where { graph <http://chuck-berry/new> { ?s ?p ?o . } }");
        TupleQueryResult totalResult = rdfRepository().query("select (count (?s) as ?count) where { graph ?g { ?s ?p ?o . filter contains(str(?g), \"chuck-berry\") . } }");
        int rejected = Integer.valueOf(rejectedResult.next().getValue("count").stringValue());
        int stillNew = Integer.valueOf(stillNewResult.next().getValue("count").stringValue());
        int total = Integer.valueOf(totalResult.next().getValue("count").stringValue());
        assertEquals(total - stillNew, rejected);
        /* Reject everything:
           1 main node (wd:Q5921 p:P18 wds:xxx) + 1 statement (wds:xxx ps:P18 URL) + 1 qualifier + 1 reference node + reference value
           Total rejected = 5
         */
        assertEquals(5, rejected);
        assertEquals(total - rejected, stillNew);
        assertEquals(22, stillNew);
    }
}
