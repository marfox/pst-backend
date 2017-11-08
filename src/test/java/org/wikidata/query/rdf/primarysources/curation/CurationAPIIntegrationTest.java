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
import java.util.Arrays;
import java.util.List;

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
        // Test failure
        builder.setParameter("qid", "Q666");
        int status = Request.Get(builder.build())
                .execute()
                .returnResponse()
                .getStatusLine()
                .getStatusCode();
        assertEquals(404, status);
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
        assertEquals(21, stillNew);
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
        assertEquals(15, stillNew);
    }

    @Test
    public void testApproveReference() throws Exception {
        // {"qs":"Q322794\tP27\tQ161885\tS854\t\"http://collection.britishmuseum.org/id/person-institution/162540\"","state":"rejected","dataset":"http://10k/new","type":"reference","user":"Hjfocs"}
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
        assertEquals(21, stillNew);
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
        assertEquals(21, stillNew);
    }
}
