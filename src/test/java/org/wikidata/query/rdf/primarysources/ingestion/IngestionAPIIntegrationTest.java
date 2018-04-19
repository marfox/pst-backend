package org.wikidata.query.rdf.primarysources.ingestion;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.primarysources.common.AbstractRdfRepositoryIntegrationTestBase;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;
import org.wikidata.query.rdf.primarysources.common.RdfVocabulary;

import com.google.common.io.Resources;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Jul 10, 2017.
 */
public class IngestionAPIIntegrationTest extends AbstractRdfRepositoryIntegrationTestBase {

    public static final String DATASET_NAME = "chuck berry";
    public static final String EXPECTED_DATASET_URI = "http://chuck-berry/new";
    public static final String DATASET_DESCRIPTION = "An amazing dataset for tests";
    public static final String UPLOADER_NAME = "IMDataProvider";
    public static final String FILE_FIELD = "dataset";
    private static final String BASE_ENDPOINT = "http://localhost:9999/bigdata";
    private static final String GOOD_DATASET_FILE_NAME = "good_chuck_berry.ttl"; // Valid data model
    private static final String PARTIALLY_BAD_DATASET_FILE_NAME = "partially_bad_chuck_berry.ttl"; // 2 valid triples
    private static final String BAD_DATASET_FILE_NAME = "bad_chuck_berry.ttl"; // Invalid data model
    private static final String BAD_RDF_FILE_NAME = "just_bad_rdf.ttl"; // Invalid RDF
    private static URI uploadEndpoint;
    private static URI updateEndpoint;
    private static File goodDataset;
    private static File partiallyBadDataset;
    private static File badDataset;
    private static File badRDF;

    private CloseableHttpClient client;

    @BeforeClass
    public static void setUpOnce() throws URISyntaxException {
        uploadEndpoint = URI.create(BASE_ENDPOINT + "/upload");
        updateEndpoint = URI.create(BASE_ENDPOINT + "/update");
        goodDataset = new File(Resources.getResource(GOOD_DATASET_FILE_NAME).toURI());
        partiallyBadDataset = new File(Resources.getResource(PARTIALLY_BAD_DATASET_FILE_NAME).toURI());
        badDataset = new File(Resources.getResource(BAD_DATASET_FILE_NAME).toURI());
        badRDF = new File(Resources.getResource(BAD_RDF_FILE_NAME).toURI());
    }

    @AfterClass
    public static void deleteCache() throws IOException {
        Files.deleteIfExists(EntitiesCache.SUBJECTS_CACHE_FILE);
        Files.deleteIfExists(EntitiesCache.PROPERTIES_CACHE_FILE);
        Files.deleteIfExists(EntitiesCache.VALUES_CACHE_FILE);
    }

    @Before
    public void setUp() {
        client = HttpClients.createDefault();
    }

    @After
    public void tearDown() throws Exception {
        client.close();
    }

    @Test
    public void testGoodDatasetUpload() throws Exception {
        String weirdDatasetName = "p:à?řó&&&l$$$É@ ;; @@  ,(é#####șÓțí**Çhê!!!?????))";
        String expectedDatasetName = "http://parole-esotiche/new";
        CloseableHttpResponse goodResponse = postDatasetUpload(uploadEndpoint, goodDataset, weirdDatasetName);
        List<String> goodResponseContent = readResponse(goodResponse);
        assertEquals(HttpServletResponse.SC_OK, goodResponse.getStatusLine().getStatusCode());
        assertEquals(1, goodResponseContent.size());
        assertTrue(rdfRepository().ask("ask where { wds:Q5921-583C7277-B344-4C96-8CF2-0557C2D0CD34 pq:P2096 \"Chuck Berry (2007)\"@ca }"));
        TupleQueryResult uploadedGraphs = rdfRepository().query("select ?g where { graph ?g { ?s ?p ?o} }");
        Set<String> namedGraphs = new HashSet<>();
        while (uploadedGraphs.hasNext()) {
            namedGraphs.add(uploadedGraphs.next().getValue("g").stringValue());
        }
        assertThat(namedGraphs, Matchers.containsInAnyOrder(expectedDatasetName, RdfVocabulary.METADATA_NAMESPACE));
        TupleQueryResult uploadedStatements = rdfRepository().query("select * where { graph <" + expectedDatasetName + "> { ?s ?p ?o } }");
        assertEquals(4, count(uploadedStatements));
        testMetadataAddition(expectedDatasetName);
    }

    private void testMetadataAddition(String expectedDatasetName) throws Exception {
        TupleQueryResult metadata = rdfRepository().query("select * where { graph <" + RdfVocabulary.METADATA_NAMESPACE + "> { ?s ?p ?o } }");
        Set<String> subjects = new HashSet<>();
        Set<String> predicates = new HashSet<>();
        Set<String> objects = new HashSet<>();
        while (metadata.hasNext()) {
            BindingSet statement = metadata.next();
            subjects.add(statement.getValue("s").stringValue());
            predicates.add(statement.getValue("p").stringValue());
            objects.add(statement.getValue("o").stringValue());
        }
        assertEquals(1, subjects.size());
        assertEquals(2, predicates.size());
        assertEquals(2, objects.size());
        assertThat(subjects, Matchers.contains(expectedDatasetName));
        assertThat(predicates, Matchers.containsInAnyOrder(RdfVocabulary.UPLOADED_BY_PREDICATE, RdfVocabulary.DESCRIPTION_PREDICATE));
        assertThat(objects, Matchers.containsInAnyOrder(RdfVocabulary.USER_URI_PREFIX + UPLOADER_NAME, DATASET_DESCRIPTION));
    }

    @Test
    public void testPartiallyBadDatasetUpload() throws Exception {
        CloseableHttpResponse partiallyBadResponse = postDatasetUpload(uploadEndpoint, partiallyBadDataset, "T3rr|i|Blę  <<>>  & dATaæš#et" +
            "     4 sure");
        String expectedDatasetName = "http://t3rrible-dataset-4-sure/new";
        List<String> partiallyBadResponseContent = readResponse(partiallyBadResponse);
        assertEquals(HttpServletResponse.SC_OK, partiallyBadResponse.getStatusLine().getStatusCode());
        assertEquals(10, partiallyBadResponseContent.size());
        assertTrue(partiallyBadResponseContent.contains("http://www.wikidata.org/prop/qualifier/I_m_not_a_valid_Item_triple"));
        assertTrue(rdfRepository().ask("ask where { wd:Q5921 p:P18 wds:Q5921-583C7277-B344-4C96-8CF2-0557C2D0CD34 }"));
        TupleQueryResult uploadedGraphs = rdfRepository().query("select ?g where { graph ?g { ?s ?p ?o} }");
        Set<String> namedGraphs = new HashSet<>();
        while (uploadedGraphs.hasNext()) {
            namedGraphs.add(uploadedGraphs.next().getValue("g").stringValue());
        }
        assertThat(namedGraphs, Matchers.containsInAnyOrder(expectedDatasetName, RdfVocabulary.METADATA_NAMESPACE));
        TupleQueryResult uploadedStatements = rdfRepository().query("select * where { graph <" + expectedDatasetName + "> { ?s ?p ?o } }");
        assertEquals(3, count(uploadedStatements));
        testSubjectItemTypeAddition(expectedDatasetName);
    }

    @Test
    public void testBadDatasetUpload() throws Exception {
        CloseableHttpResponse badResponse = postDatasetUpload(uploadEndpoint, badDataset, "baddataset");
        assertEquals(HttpServletResponse.SC_ACCEPTED, badResponse.getStatusLine().getStatusCode());
        assertFalse(rdfRepository().ask("ask where {?s ?p ?o}"));
        assertFalse(rdfRepository().query("select * where {?s ?p ?o}").hasNext());
    }

    @Test
    public void testBadRDFUpload() throws Exception {
        CloseableHttpResponse badRDFResponse = postDatasetUpload(uploadEndpoint, badRDF, "badrdf");
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, badRDFResponse.getStatusLine().getStatusCode());
    }

    // Remove good dataset, add good dataset
    @Test
    public void testGoodDatasetUpdate() throws Exception {
        postDatasetUpload(uploadEndpoint, goodDataset, DATASET_NAME);
        CloseableHttpResponse goodResponse = postDatasetUpdate(updateEndpoint, goodDataset, goodDataset);
        List<String> goodResponseContent = readResponse(goodResponse);
        assertTrue(goodResponseContent.isEmpty());
        assertEquals(HttpServletResponse.SC_OK, goodResponse.getStatusLine().getStatusCode());
        assertTrue(rdfRepository().ask("ask where { wdref:288ab581e7d2d02995a26dfa8b091d96e78457fc pr:P143 wd:Q206855 }"));
        TupleQueryResult updatedStatements = rdfRepository().query("select * where { graph <" + EXPECTED_DATASET_URI + "> { ?s ?p ?o } }");
        assertEquals(4, count(updatedStatements));
    }

    // Remove good dataset, add partially bad dataset
    @Test
    public void testPartiallyBadDatasetUpdate() throws Exception {
        postDatasetUpload(uploadEndpoint, goodDataset, DATASET_NAME);
        CloseableHttpResponse partiallyBadResponse = postDatasetUpdate(updateEndpoint, goodDataset, partiallyBadDataset);
        String expectedDatasetName = EXPECTED_DATASET_URI;
        List<String> partiallyBadResponseContent = readResponse(partiallyBadResponse);
        assertEquals(10, partiallyBadResponseContent.size());
        assertEquals(HttpServletResponse.SC_OK, partiallyBadResponse.getStatusLine().getStatusCode());
        assertTrue(rdfRepository().ask("ask where { wd:Q5921 p:P18 wds:Q5921-583C7277-B344-4C96-8CF2-0557C2D0CD34 }"));
        TupleQueryResult updatedStatements = rdfRepository().query("select * where { graph <" + expectedDatasetName + "> { ?s ?p ?o } }");
        assertEquals(3, count(updatedStatements));
        testSubjectItemTypeAddition(expectedDatasetName);
    }

    // Remove good dataset, add bad dataset
    @Test
    public void testBadDatasetUpdate() throws Exception {
        postDatasetUpload(uploadEndpoint, goodDataset, DATASET_NAME);
        CloseableHttpResponse badResponse = postDatasetUpdate(updateEndpoint, goodDataset, badDataset);
        List<String> badResponseContent = readResponse(badResponse);
        assertEquals(12, badResponseContent.size());
        assertEquals(HttpServletResponse.SC_OK, badResponse.getStatusLine().getStatusCode());
        assertFalse(rdfRepository().ask("ask where { graph <" + EXPECTED_DATASET_URI + "> { ?s ?p ?o } }"));
        assertFalse(rdfRepository().query("select * where { graph <" + EXPECTED_DATASET_URI + "> { ?s ?p ?o } }").hasNext());
    }

    // Remove good dataset, add bad RDF
    @Test
    public void testBadRDFUpdate() throws Exception {
        postDatasetUpload(uploadEndpoint, goodDataset, DATASET_NAME);
        CloseableHttpResponse badRDFResponse = postDatasetUpdate(updateEndpoint, goodDataset, badRDF);
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, badRDFResponse.getStatusLine().getStatusCode());
        // Check that nothing happened, i.e., that the good dataset is still there
        TupleQueryResult uploadedStatements = rdfRepository().query("select * where { graph <" + EXPECTED_DATASET_URI + "> { ?s ?p ?o } }");
        assertEquals(4, count(uploadedStatements));
    }

    private int count(TupleQueryResult result) throws Exception {
        int total = 0;
        while (result.hasNext()) {
            result.next();
            total++;
        }
        return total;
    }

    private void testSubjectItemTypeAddition(String datasetUri) throws Exception {
        TupleQueryResult typeTriples = rdfRepository().query("select * where { graph <" + datasetUri + "> { ?s a <" + Ontology.ITEM + "> } }");
        assertEquals(1, count(typeTriples));
    }

    private List<String> readResponse(CloseableHttpResponse response) throws IOException {
        List<String> responseContent = new ArrayList<>();
        HttpEntity responseEntity = response.getEntity();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(responseEntity.getContent(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                responseContent.add(line);
            }
        }
        return responseContent;
    }

    private CloseableHttpResponse postDatasetUpload(URI endpoint, File dataset, String datasetName) throws IOException {
        HttpPost post = new HttpPost(endpoint);
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        ContentType textContentType = ContentType.create("text/plain", StandardCharsets.UTF_8);
        builder.addTextBody(ApiParameters.DATASET_NAME_FORM_FIELD, datasetName, textContentType);
        builder.addTextBody(ApiParameters.DATASET_DESCRIPTION_FORM_FIELD, DATASET_DESCRIPTION, textContentType);
        builder.addTextBody(ApiParameters.USER_NAME_PARAMETER, UPLOADER_NAME, textContentType);
        builder.addBinaryBody(FILE_FIELD, dataset);
        HttpEntity datasetUpload = builder.build();
        post.setEntity(datasetUpload);
        return client.execute(post);
    }

    private CloseableHttpResponse postDatasetUpdate(URI endpoint, File toRemove, File toAdd) throws IOException {
        HttpPost post = new HttpPost(endpoint);
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        ContentType textContentType = ContentType.create("text/plain", StandardCharsets.UTF_8);
        builder.addTextBody(ApiParameters.DATASET_PARAMETER, EXPECTED_DATASET_URI, textContentType);
        builder.addTextBody(ApiParameters.USER_NAME_PARAMETER, UPLOADER_NAME, textContentType);
        builder.addBinaryBody(ApiParameters.REMOVE_FORM_FIELD, toRemove);
        builder.addBinaryBody(ApiParameters.ADD_FORM_FIELD, toAdd);
        HttpEntity update = builder.build();
        post.setEntity(update);
        return client.execute(post);
    }

}
