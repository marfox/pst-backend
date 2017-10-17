package org.wikidata.query.rdf.primarysources.ingestion;

import com.google.common.io.Resources;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.openrdf.model.Model;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.WikibaseDataModelValidator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.Normalizer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Jul 04, 2017.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class UploadServlet extends HttpServlet {

    /**
     * Endpoint name of the Blazegraph SPARQL service.
     */
    public static final String BLAZEGRAPH_SPARQL_ENDPOINT = "/sparql";
    /**
     * The data provider should not care about the base URI. A constant is used instead.
     */
    static final String BASE_URI = "https://www.wikidata.org";
    /**
     * The less verbose RDF format is the default.
     */
    static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.TURTLE;
    /**
     * Expected HTTP form field with the Wiki user name of the dataset provider.
     */
    static final String USER_NAME_FORM_FIELD = "user";
    /**
     * Expected HTTP form field with the name of the dataset.
     */
    private static final String DATASET_NAME_FORM_FIELD = "name";
    /**
     * The uploaded dataset must be saved to the server local file system, before sending it to the Blazegraph bulk load service.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     */
    private static final String TEMP_DATASET_FILE_NAME = "to_be_uploaded";
    /**
     * Endpoint name of the Blazegraph bulk load service.
     */
    private static final String BLAZEGRAPH_DATA_LOADER_ENDPOINT = "/dataloader";
    /**
     * Blazegraph database instance configuration file name.
     */
    private static final String BLAZEGRAPH_PROPERTIES_FILE_NAME = "RWStore.properties";
    private static final Logger log = LoggerFactory.getLogger(UploadServlet.class);

    /**
     * {@link Properties} required for the Blazegraph bulk load service, to set up the database instance.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     * <p>
     * The data provider should not care about this.
     */
    private String blazegraphPropertiesLocation;
    /**
     * {@link Properties} required for the Blazegraph bulk load service, to set up the data loader.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     * <p>
     * The data provider should not care about this.
     */
    private Properties dataLoaderProperties;
    /**
     * Temporary file with the uploaded dataset to be stored in the server local file system.
     * The uploaded dataset must be saved to the server local file system, before sending it to the Blazegraph bulk load service.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     */
    private File tempDataset;

    /**
     * Upload a RDF dataset to Blazegraph, upon Wikidata data model validation.
     *
     * @param request  the client HTTP request
     * @param response the servlet HTTP response
     * @throws IOException      if an input or output error is detected when the servlet handles the request
     * @throws ServletException if the request for the POST could not be handled
     */
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        RDFFormat format = null;
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        Model validSyntax = null;
        URI datasetURI = null;
        String user = null;
        // Check that we have a file upload request
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);
        if (isMultipart) {
            // Create a new file upload handler
            ServletFileUpload upload = new ServletFileUpload();
            // Parse the request
            FileItemIterator iter;
            try {
                iter = upload.getItemIterator(request);
                while (iter.hasNext()) {
                    FileItemStream item = iter.next();
                    String fieldName = item.getFieldName();
                    InputStream stream = item.openStream();
                    if (item.isFormField()) {
                        String formValue = Streams.asString(stream);
                        switch (fieldName) {
                        case DATASET_NAME_FORM_FIELD:
                            log.info("Dataset form field '{}' detected. Will build a sanitized ASCII URI out of value '{}' as the named graph where the " +
                                "dataset will be stored.", fieldName, formValue);
                            datasetURI = mintDatasetURI(formValue);
                            dataLoaderProperties.setProperty("defaultGraph", datasetURI.toString());
                            break;
                        case USER_NAME_FORM_FIELD:
                            log.info("User name form field '{}' detected. Will store the value '{}' as the uploader of the dataset", fieldName, formValue);
                            user = formValue;
                            break;
                        default:
                            log.error("Unexpected form field '{}' with value '{}'. Will fail with a a bad request", fieldName, formValue);
                            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected form field '" + fieldName + "' with value '" + formValue + "'");
                            return;
                        }
                    } else {
                        String fileName = item.getName();
                        log.info("File field '" + fieldName + "' with file name '" + fileName + "' detected.");
                        /*
                         * Guess the RDF format based on the file name extension.
                         * This is the only solution, as the content type is multipart/form-data.
                         * Fall back to Turtle if the guess fails, as we cannot blame the user for uploading proper content with an arbitrary (or no) extension.
                         */
                        format = Rio.getParserFormatForFileName(fileName, DEFAULT_RDF_FORMAT);
                        // 1. Validate syntax
                        try {
                            validSyntax = validator.checkSyntax(stream, BASE_URI, format);
                        } catch (RDFParseException rpe) {
                            log.error("The dataset is not valid RDF. Error at line {}, column {}. Will fail with a bad request", rpe.getLineNumber(), rpe
                                .getColumnNumber());
                            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Your dataset is not valid RDF. Found an error at line " + rpe
                                .getLineNumber() +
                                ", " +
                                "column " + rpe.getColumnNumber() +
                                ". Please fix it and try again");
                            return;
                        }
                    }
                }
            } catch (FileUploadException fue) {
                log.error("Failed reading/parsing the request or storing files: {}", fue);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, fue.getLocalizedMessage());
                return;
            }
        } else {
            String actualContentType = request.getContentType();
            log.error("Not a multipart content type: {} Will fail with a bad request", actualContentType);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "You should upload your dataset as a file using multipart/form-data content type, not " +
                actualContentType +
                ". Please fix your HTTP request and try again.");
            return;
        }

        // 2. Validate the data model
        AbstractMap.SimpleImmutableEntry<Model, List<String>> validated = validator.handleDataset(validSyntax);
        Model toBeUploaded = validated.getKey();
        List<String> invalid = validated.getValue();
        upload(request, response, format, datasetURI, user, toBeUploaded, invalid);
    }

    @Override
    public void destroy() {
        tempDataset.delete();
    }

    @Override
    public void init() throws ServletException {
        try {
            blazegraphPropertiesLocation = Resources.getResource(BLAZEGRAPH_PROPERTIES_FILE_NAME).toURI().getPath();
        } catch (URISyntaxException use) {
            log.error("Could not parse the Blazegraph properties file URI: {} Parse error at index {}", use.getInput(), use.getIndex());
        }
        dataLoaderProperties = new Properties();
        dataLoaderProperties.setProperty("quiet", "false");
        dataLoaderProperties.setProperty("verbose", "0");
        dataLoaderProperties.setProperty("closure", "false");
        dataLoaderProperties.setProperty("durableQueues", "true");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.flush", "false");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.bufferCapacity", "100000");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.queueCapacity", "10");
        dataLoaderProperties.setProperty("namespace", "wdq");
        dataLoaderProperties.setProperty("propertyFile", blazegraphPropertiesLocation);
    }

    /**
     * Upload the dataset to Blazegraph and build the HTTP response.
     * If there are no valid triples, the request is still correct, but the server does not upload anything to Blazegraph.
     *
     * @throws IOException if an input or output error is detected when uploading the dataset
     */
    private void upload(HttpServletRequest request, HttpServletResponse response, RDFFormat format, URI datasetURI, String user, Model toBeUploaded,
                        List<String> invalid) throws IOException {
        if (!toBeUploaded.isEmpty()) {
            // The data loader needs a file to be stored on the server local file system
            try {
                // Set a suitable extension based on the RDF format
                tempDataset = File.createTempFile(TEMP_DATASET_FILE_NAME, "." + format.getDefaultFileExtension());
                Rio.write(toBeUploaded, Files.newBufferedWriter(tempDataset.toPath(), StandardCharsets.UTF_8), format);
            } catch (RDFHandlerException rhe) {
                log.error("Failed writing RDF: {}", rhe);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, rhe.getLocalizedMessage());
                return;
            }
            // 3. Send the dataset to the Blazegraph bulk load service
            AbstractMap.SimpleImmutableEntry<Integer, List<String>> dataLoaderResponse = sendDatasetToDataLoader(request);
            // 4. Add the (user, uploaded, dataset) statement to the metadata named graph
            int quadAdditionResponseCode = addUploaderQuad(request, user, datasetURI.toString());
            if (quadAdditionResponseCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR) {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Failed adding metadata that links your user name to the dataset you uploaded.");
                return;
            }
            // 5. Send the final response, including the list of invalid triples and the response content from the bulk load service
            sendResponse(response, invalid, dataLoaderResponse);
        } else {
            log.warn("The request succeeded, but no content passed the data model validation. Nothing will be uploaded to Blazegraph");
            response.sendError(HttpServletResponse.SC_ACCEPTED, "Your dataset has no content that complies with the Wikidata RDF data model. Nothing will be" +
                " uploaded. Please check the <a href=\"https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model\">documentation</a> and " +
                "try again.");
        }
    }

    /**
     * Build a sanitized ASCII URI out of a given dataset name.
     */
    private URI mintDatasetURI(String datasetName) {
        // Delete any character that is not a letter, a number, or a whitespace, as we want to mint readable URIs
        String onlyLetters = datasetName.replaceAll("[^\\p{L}\\d\\s]", "");
        // Remove diactrics to mint ASCII URIs
        String noDiacritics = Normalizer.normalize(onlyLetters, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        // Replace whitespaces with a dash
        String clean = noDiacritics.replaceAll("\\s+", "-");
        // A freshly uploaded dataset gets the "/new" state
        URI datasetURI = URI.create("http://" + clean.toLowerCase(Locale.ENGLISH) + "/new");
        log.info("Named graph URI: {}", datasetURI);
        return datasetURI;
    }

    /**
     * @param response           the servlet HTTP response
     * @param invalid            the list of invalid data
     * @param dataLoaderResponse the Blazegraph data loader servlet HTTP response
     * @throws IOException if an error occurs while getting the response output writer
     */
    private void sendResponse(HttpServletResponse response, List<String> invalid, AbstractMap.SimpleImmutableEntry<Integer, List<String>> dataLoaderResponse)
        throws IOException {
        // The final response code is the data loader one
        int dataLoaderResponseCode = dataLoaderResponse.getKey();
        List<String> dataLoaderResponseContent = dataLoaderResponse.getValue();
        response.setStatus(dataLoaderResponseCode);
        response.setContentType("text/plain");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        PrintWriter pw = response.getWriter();
        for (String invalidComponent : invalid) {
            pw.println(invalidComponent);
        }
        if (!dataLoaderResponseContent.isEmpty()) {
            for (String dataLoaderResponseLine : dataLoaderResponse.getValue()) {
                pw.println(dataLoaderResponseLine);
            }
        }
        pw.close();
    }

    /**
     * Send the uploaded dataset to the Blazegraph bulk load service, firing a POST with the required request.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     * Alternative solutions may be:
     * B. use a HttpServletRequestWrapper, i.e., wrapper = new HttpServletRequestWrapper(request);
     * C. use a Filter
     *
     * @param request the client HTTP request
     * @throws IOException if an input or output error is detected when the client sends the request to the data loader servlet
     */
    private AbstractMap.SimpleImmutableEntry<Integer, List<String>> sendDatasetToDataLoader(HttpServletRequest request) throws IOException {
        List<String> responseContent = new ArrayList<>();
        URL url = new URL(request.getRequestURL().toString().replace(request.getServletPath(), BLAZEGRAPH_DATA_LOADER_ENDPOINT));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "text/plain");
        connection.setDoOutput(true);
        DataOutputStream dos = new DataOutputStream(connection.getOutputStream());
        dataLoaderProperties.setProperty("fileOrDirs", tempDataset.getPath());
        dataLoaderProperties.store(dos, "Expected properties for the Blazegraph data loader service");
        dos.close();
        // Check that everything went fine
        int responseCode = connection.getResponseCode();
        InputStream responseStream;
        // Get the data loader response only if it went wrong
        if (responseCode == HttpServletResponse.SC_OK) {
            log.info("The dataset ingestion into Blazegraph went fine");
        } else {
            log.error("Failed ingesting the dataset into Blazegraph, HTTP error code: {}", responseCode);
            responseStream = connection.getErrorStream();
            BufferedReader responseReader = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8));
            String line;
            while ((line = responseReader.readLine()) != null) {
                responseContent.add(line);
            }
            responseReader.close();
        }
        connection.disconnect();
        return new AbstractMap.SimpleImmutableEntry<>(responseCode, responseContent);
    }

    /**
     * Add the (user, uploaded, dataset) statement to the metadata named graph.
     *
     * @param request
     * @param user
     * @param dataset
     * @return
     * @throws IOException if an input or output error is detected when the client sends the request to the SPARQL service
     */
    private int addUploaderQuad(HttpServletRequest request, String user, String dataset) throws IOException {
        // Reliably build the quad
        String wikidataNamespace = WikibaseUris.WIKIDATA.root();
        String primarySourcesNamespace = wikidataNamespace + "/primary-sources";
        ValueFactory vf = ValueFactoryImpl.getInstance();
        org.openrdf.model.URI subject = vf.createURI(wikidataNamespace, "/wiki/User:" + user);
        org.openrdf.model.URI predicate = vf.createURI(primarySourcesNamespace, "/uploaded");
        org.openrdf.model.URI object = vf.createURI(dataset);
        org.openrdf.model.URI context = vf.createURI(primarySourcesNamespace);
        // Fire the POST
        URL url = new URL(request.getRequestURL().toString().replace(request.getServletPath(), BLAZEGRAPH_SPARQL_ENDPOINT));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "text/x-nquads");
        connection.setDoOutput(true);
        BufferedWriter bf = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8));
        String statement = "<" + subject.stringValue() + "> <" + predicate.stringValue() + "> <" + object.stringValue() + "> <" + context.stringValue() + "> .";
        bf.write(statement);
        bf.flush();
        bf.close();
        int responseCode = connection.getResponseCode();
        connection.disconnect();
        return responseCode;
    }
}
