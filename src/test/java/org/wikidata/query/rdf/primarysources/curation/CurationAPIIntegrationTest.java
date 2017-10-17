package org.wikidata.query.rdf.primarysources.curation;

import com.google.common.io.Resources;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.hamcrest.Matchers;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.primarysources.AbstractRdfRepositoryIntegrationTestBase;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Oct 10, 2017.
 */
public class CurationAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    private static final String BASE_ENDPOINT = "http://localhost:9999/bigdata";
    private static final String TEST_DATASET_FILE_NAME = "chuck_berry_for_curation.ttl";
    private static final String TEST_QID = "Q5921";

    private static URI uploadEndpoint;
    private static URI suggestEndpoint;
    private static URI curateEndpoint;
    private static File testDataset;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        uploadEndpoint = URI.create(BASE_ENDPOINT + "/upload");
        suggestEndpoint = URI.create(BASE_ENDPOINT + "/suggest");
        curateEndpoint = URI.create(BASE_ENDPOINT + "/curate");
        testDataset = new File(Resources.getResource(TEST_DATASET_FILE_NAME).toURI());
    }

    @Before
    public void setUp() throws Exception {
        MultipartEntityBuilder multipart = MultipartEntityBuilder.create();
        multipart.addTextBody("name", "chuck berry", ContentType.TEXT_PLAIN);
        multipart.addTextBody("user", "IMDataProvider", ContentType.TEXT_PLAIN);
        multipart.addBinaryBody("dataset", testDataset);
        Request.Post(uploadEndpoint)
            .body(multipart.build())
            .execute()
            .discardContent();
    }

    @Test
    public void testSuggest() throws Exception {
        URIBuilder builder = new URIBuilder(suggestEndpoint);
        // Test success
        builder.setParameter("qid", TEST_QID);
        String responseContent = Request.Get(builder.build())
            .execute()
            .returnContent()
            .asString();
        JSONParser parser = new JSONParser();
        Object parsed = parser.parse(responseContent);
        Assert.assertThat(parsed, Matchers.instanceOf(JSONArray.class));
        JSONArray suggestions = (JSONArray) parsed;
        Assert.assertEquals(13, suggestions.size());
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
            JSONObject suggestion = (JSONObject) s;
            String type = (String) suggestion.get("type");
            switch (type) {
            case "claim":
                actualStatements += 1;
                break;
            case "qualifier":
                actualQualifiers += 1;
                break;
            case "reference":
                actualReferences += 1;
                break;
            }
        }
        Assert.assertEquals(expectedStatements, actualStatements);
        Assert.assertEquals(expectedQualifers, actualQualifiers);
        Assert.assertEquals(expectedReferences, actualReferences);
        // Test failure
        builder.setParameter("qid", "Q666");
        int status = Request.Get(builder.build())
            .execute()
            .returnResponse()
            .getStatusLine()
            .getStatusCode();
        Assert.assertEquals(404, status);
    }

    @Test
    public void testApproveClaim() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qid", TEST_QID);
        curated.put("main_property", "P999");
        JSONObject forMWAPI = new JSONObject();
        forMWAPI.put("property", "P999");
        forMWAPI.put("snaktype", "value");
        forMWAPI.put("value", "Maybelline");
        curated.put("for_mw_api", forMWAPI);
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
        Assert.assertEquals(total - stillNew, approved);
        Assert.assertEquals(5, approved);
        Assert.assertEquals(total - approved, stillNew);
        Assert.assertEquals(15, stillNew);
    }

    @Test
    public void testRejectClaim() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qid", TEST_QID);
        curated.put("main_property", "P999");
        JSONObject forMWAPI = new JSONObject();
        forMWAPI.put("property", "P999");
        forMWAPI.put("snaktype", "value");
        forMWAPI.put("value", "Maybelline");
        curated.put("for_mw_api", forMWAPI);
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
        Assert.assertEquals(total - stillNew, rejected);
        Assert.assertEquals(10, rejected);
        Assert.assertEquals(total - rejected, stillNew);
        Assert.assertEquals(10, stillNew);
    }

    @Test
    public void testApproveReference() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qid", TEST_QID);
        curated.put("main_property", "P18");
        JSONObject forMWAPI = new JSONObject();
        JSONObject snaks = new JSONObject();
        JSONArray values = new JSONArray();
        JSONObject value = new JSONObject();
        JSONObject datavalue = new JSONObject();
        datavalue.put("type", "string");
        datavalue.put("value", "https://travisraminproducer.bandcamp.com/");
        value.put("datavalue", datavalue);
        value.put("property", "P854");
        value.put("snaktype", "value");
        values.add(value);
        snaks.put("P854", values);
        forMWAPI.put("snaks", snaks);
        curated.put("for_mw_api", forMWAPI);
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
        Assert.assertEquals(total - stillNew, approved);
        Assert.assertEquals(5, approved);
        Assert.assertEquals(total - approved, stillNew);
        Assert.assertEquals(15, stillNew);
    }

    @Test
    public void testRejectReference() throws Exception {
        JSONObject curated = new JSONObject();
        curated.put("qid", TEST_QID);
        curated.put("main_property", "P18");
        JSONObject forMWAPI = new JSONObject();
        JSONObject snaks = new JSONObject();
        JSONArray values = new JSONArray();
        JSONObject value = new JSONObject();
        JSONObject datavalue = new JSONObject();
        datavalue.put("type", "string");
        datavalue.put("value", "https://travisraminproducer.bandcamp.com/");
        value.put("datavalue", datavalue);
        value.put("property", "P854");
        value.put("snaktype", "value");
        values.add(value);
        snaks.put("P854", values);
        forMWAPI.put("snaks", snaks);
        curated.put("for_mw_api", forMWAPI);
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
        Assert.assertEquals(total - stillNew, rejected);
        Assert.assertEquals(5, rejected);
        Assert.assertEquals(total - rejected, stillNew);
        Assert.assertEquals(15, stillNew);
    }
}
