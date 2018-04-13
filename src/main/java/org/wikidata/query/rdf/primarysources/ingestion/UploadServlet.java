package org.wikidata.query.rdf.primarysources.ingestion;

import com.google.common.io.Resources;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.Normalizer;
import java.util.*;

import static org.wikidata.query.rdf.common.uri.Ontology.ITEM;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.WIKIBASE_URIS;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Jul 04, 2017.
 */
public class UploadServlet extends HttpServlet {

    /**
     * Endpoint name of the Blazegraph SPARQL service.
     */
    public static final String BLAZEGRAPH_SPARQL_ENDPOINT = "/sparql";
    public static final String BLAZEGRAPH_HOST = System.getenv("HOST");
    public static final int BLAZEGRAPH_PORT = Integer.parseInt(System.getenv("PORT"));
    public static final String BLAZEGRAPH_CONTEXT = "/" + System.getenv("CONTEXT");
    /**
     * The data provider should not care about the base URI. A constant is used instead.
     */
    static final String BASE_URI = WikibaseUris.WIKIDATA.root();
    /**
     * Namespace URI for metadata triples. Used to store data providers and users activities.
     * See {@link UploadServlet#addMetadataQuads(RequestParameters, HttpServletResponse)} and {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String METADATA_NAMESPACE = BASE_URI + "/primary-sources";
    public static final String DESCRIPTION_PREDICATE = METADATA_NAMESPACE + "/description";
    public static final String UPLOADED_BY_PREDICATE = METADATA_NAMESPACE + "/uploadedBy";
    /**
     * Prefix URI for users. Append the user name to build a full user URI.
     */
    public static final String USER_URI_PREFIX = BASE_URI + "/wiki/User:";
    /**
     * The less verbose RDF format is the default.
     */
    static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.TURTLE;
    /**
     * Expected HTTP form field with the Wiki user name of the dataset provider.
     */
    public static final String USER_NAME_FORM_FIELD = "user";
    /**
     * Expected HTTP form field with the name of the dataset.
     */
    public static final String DATASET_NAME_FORM_FIELD = "name";
    /**
     * Optional HTTP form field with the dataset description.
     */
    public static final String DATASET_DESCRIPTION_FORM_FIELD = "description";
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

    private class RequestParameters {
        public Properties dataLoaderProperties;
        public String user;
        public String datasetFileName;
        public String datasetURI;
        public String datasetDescription;
    }

    /**
     * {@link Properties} required for the Blazegraph bulk load service, to set up the data loader.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     * <p>
     * The data provider should not care about this.
     */
    private Properties buildDataLoaderProperties() {
        // Properties required for the Blazegraph bulk load service, to set up the database instance.
        String blazegraphPropertiesLocation = null;
        try {
            blazegraphPropertiesLocation = Resources.getResource(BLAZEGRAPH_PROPERTIES_FILE_NAME).toURI().getPath();
        } catch (URISyntaxException use) {
            log.error("Could not parse the Blazegraph properties file URI: {} Parse error at index {}", use.getInput(), use.getIndex());
        }
        Properties dataLoaderProperties = new Properties();
        dataLoaderProperties.setProperty("quiet", "false");
        dataLoaderProperties.setProperty("verbose", "0");
        dataLoaderProperties.setProperty("closure", "false");
        dataLoaderProperties.setProperty("durableQueues", "true");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.flush", "false");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.bufferCapacity", "100000");
        dataLoaderProperties.setProperty("com.bigdata.rdf.store.DataLoader.queueCapacity", "10");
        dataLoaderProperties.setProperty("namespace", "wdq");
        dataLoaderProperties.setProperty("propertyFile", blazegraphPropertiesLocation);
        return dataLoaderProperties;
    }

    /**
     * Upload a RDF dataset to Blazegraph, upon Wikidata data model validation.
     *
     * @param request  the client HTTP request
     * @param response the servlet HTTP response
     * @throws IOException if an input or output error is detected when the servlet handles the request
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        parameters.dataLoaderProperties = buildDataLoaderProperties();
        log.debug("Initial properties for the Blazegraph data loader: {}. Stored in: {}", parameters.dataLoaderProperties, parameters);
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        Map<String, AbstractMap.SimpleImmutableEntry<RDFFormat, Model>> validRDFDatasets = new HashMap<>();
        List<File> tempDatasets = new ArrayList<>();
        Map<String, List<String>> invalidComponents = new HashMap<>();
        List<String> notUploaded = new ArrayList<>();
        boolean ok = processRequest(request, response, validator, validRDFDatasets, parameters);
        if (!ok) return;
        log.debug("RDF files with valid syntax: {}. They will undergo data model validation.", validRDFDatasets);
        for (String dataset : validRDFDatasets.keySet()) {
            AbstractMap.SimpleImmutableEntry<RDFFormat, Model> valid = validRDFDatasets.get(dataset);
            AbstractMap.SimpleImmutableEntry<Model, List<String>> validated = validator.handleDataset(valid.getValue());
            Model toBeUploaded = validated.getKey();
            if (toBeUploaded.isEmpty()) {
                log.warn("Dataset '{}': no content passed the data model validation. It will not be uploaded to Blazegraph", dataset);
                notUploaded.add(dataset);
                continue;
            }
            invalidComponents.put(dataset, validated.getValue());
            addTypeToSubjectItems(toBeUploaded, parameters.datasetURI);
            File tempDataset = writeTempDataset(response, valid.getKey(), toBeUploaded);
            if (tempDataset == null) return;
            tempDatasets.add(tempDataset);
        }
        if (tempDatasets.isEmpty()) {
            log.warn("No file passed the data model validation. Will fail with a 202 status code");
            response.sendError(HttpServletResponse.SC_ACCEPTED, "The request succeeded, but no content complies with the Wikidata RDF data model." +
                    "Nothing will be uploaded. Please check the " +
                    "<a href=\"https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model\">documentation</a> and try again.");
            return;
        }
        log.debug("Valid files that will be uploaded: {}", tempDatasets);
        AbstractMap.SimpleImmutableEntry<Integer, List<String>> dataLoaderResponse = sendDatasetsToDataLoader(tempDatasets, parameters, response);
        if (dataLoaderResponse == null) return;
        boolean added = addMetadataQuads(parameters, response);
        if (!added) return;
        for (File tempDataset : tempDatasets) tempDataset.delete();
        log.debug("Temporary dataset files deleted");
        EntitiesCache.cacheDatasetEntities(parameters.datasetURI);
        sendResponse(response, notUploaded, invalidComponents, dataLoaderResponse);
        log.info("POST /upload successful");
    }

    static void addTypeToSubjectItems(Model dataset, String uri) {
        Set<Resource> subjects = dataset.subjects();
        Set<org.openrdf.model.URI> items = new HashSet<>();
        for (Resource s : subjects) {
            org.openrdf.model.URI subject = (org.openrdf.model.URI) s;
            if (subject.getNamespace().equals(WIKIBASE_URIS.entity())) items.add(subject);
        }
        for (org.openrdf.model.URI item : items) dataset.add(item, RDF.TYPE, new URIImpl(ITEM), new URIImpl(uri));
        log.debug("Added a (item, rdf:type, wikibase:Item) triple to each subject item: {}", items);
    }

    /**
     * The Blazegraph data loader needs a file to be stored on the server local file system, so use a temporary file.
     *
     * @throws IOException if an error is detected when writing the temp
     */
    private File writeTempDataset(HttpServletResponse response, RDFFormat format, Model toBeUploaded) throws IOException {
        File tempDataset;
        try {
            tempDataset = File.createTempFile(TEMP_DATASET_FILE_NAME, "." + format.getDefaultFileExtension());
            Rio.write(toBeUploaded, Files.newBufferedWriter(tempDataset.toPath(), StandardCharsets.UTF_8), format);
        } catch (RDFHandlerException rhe) {
            log.error("Failed writing RDF: {}", rhe);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, rhe.getLocalizedMessage());
            return null;
        }
        log.debug("Temporary dataset file written: {}", tempDataset);
        return tempDataset;
    }

    /**
     * Handle the request and check the RDF syntax of the given datasets, firing appropriate error codes when necessary.
     *
     * @throws IOException if an error is detected when operating on the form fields.
     */
    private boolean processRequest(HttpServletRequest request, HttpServletResponse response, WikibaseDataModelValidator validator, Map<String, AbstractMap
            .SimpleImmutableEntry<RDFFormat, Model>>
            validRDFDatasets, RequestParameters parameters) throws IOException {
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);
        if (isMultipart) {
            ServletFileUpload upload = new ServletFileUpload();
            FileItemIterator iter;
            try {
                iter = upload.getItemIterator(request);
                while (iter.hasNext()) {
                    FileItemStream item = iter.next();
                    try (InputStream fieldStream = item.openStream()) {
                        if (item.isFormField()) {
                            boolean handled = handleFormField(item, fieldStream, parameters, response);
                            if (!handled) return false;
                        } else {
                            AbstractMap.SimpleImmutableEntry<RDFFormat, Model> valid = handleFileField(item, fieldStream, parameters, validator, response);
                            if (valid == null) return false;
                            else {
                                validRDFDatasets.put(item.getName(), valid);
                            }
                        }
                    }
                }
                boolean checked = checkRequiredFields(parameters, response);
                if (!checked) return false;
                log.debug("Required parameters stored as fields in private class: {}", parameters);
            } catch (FileUploadException fue) {
                log.error("Failed reading/parsing the request or storing files: {}", fue);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, fue.getLocalizedMessage());
                return false;
            }
        } else {
            String actualContentType = request.getContentType();
            log.warn("Not a multipart content type: {} Will fail with a bad request", actualContentType);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "You should upload your dataset as a file using multipart/form-data content type, not " +
                    actualContentType + ". Please fix your HTTP request and try again.");
            return false;
        }
        return true;
    }

    private boolean checkRequiredFields(RequestParameters parameters, HttpServletResponse response) throws IOException {
        if (parameters.user == null) {
            log.warn("No user name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No user name given. Please use the field '" + USER_NAME_FORM_FIELD + "' to send it.");
            return false;
        }
        if (parameters.datasetFileName == null) {
            log.warn("No dataset file given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset file given. Please upload at least a file.");
            return false;
        }
        if (parameters.datasetURI == null) {
            log.warn("No dataset name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset name given. Please use the field '" + DATASET_NAME_FORM_FIELD + "' to send it.");
            return false;
        }
        if (parameters.datasetDescription == null) {
            log.info("No dataset description given.");
        }
        return true;
    }

    /**
     * Process a form field uploaded by the client, firing a bad request if it is not expected.
     *
     * @throws IOException in case of troubles when reading the field value or sending the bad request
     */
    private boolean handleFormField(FileItemStream item, InputStream fieldStream, RequestParameters parameters, HttpServletResponse response) throws IOException {
        String field = item.getFieldName();
        String value = Streams.asString(fieldStream);
        switch (field) {
            case DATASET_NAME_FORM_FIELD:
                log.info("Dataset name detected. Will build a sanitized ASCII URI out of value '{}' as the named graph where the " +
                        "dataset will be stored.", value);
                parameters.datasetURI = mintDatasetURI(value);
                String defaultGraph = "defaultGraph";
                parameters.dataLoaderProperties.setProperty(defaultGraph, parameters.datasetURI);
                log.debug("Named graph URI added to the Blazegraph data loader properties: {} = {}", defaultGraph, parameters.dataLoaderProperties.getProperty(defaultGraph));
                return true;
            case DATASET_DESCRIPTION_FORM_FIELD:
                log.info("Dataset description detected. Will be stored in the metadata graph <{}>", METADATA_NAMESPACE);
                parameters.datasetDescription = value;
                log.debug("Named graph URI added to the Blazegraph data loader properties: {} = {}", parameters.datasetDescription);
                return true;
            case USER_NAME_FORM_FIELD:
                log.info("User name detected. Will store the value '{}' as the uploader of the dataset", value);
                parameters.user = value;
                return true;
            default:
                log.warn("Unexpected form field '{}' with value '{}'. Will fail with a a bad request", field, value);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected form field '" + field + "' with value '" + value + "'");
                return false;
        }
    }

    /**
     * Process a file uploaded by the client, firing a bad request if the format is not recognized as RDF.
     * If a file looks like RDF, then check its syntax.
     *
     * @throws IOException if an error is detected when operating on the file
     */
    private AbstractMap.SimpleImmutableEntry<RDFFormat, Model> handleFileField(FileItemStream item, InputStream fieldStream, RequestParameters parameters,
                                                                               WikibaseDataModelValidator validator, HttpServletResponse response) throws IOException {
        String fieldName = item.getFieldName();
        String fileName = item.getName();
        String contentType = item.getContentType();
        log.info("File field '{}' with file name '{}' detected.", fieldName, fileName);
        parameters.datasetFileName = fileName;
        RDFFormat format = handleFormat(contentType, fileName);
        if (format == null) {
            log.warn("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                    fileName, contentType);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset '" + fileName +
                    "' does not match any RDF format. Both the file content type '" + contentType + "' and the extension are invalid. Please fix this and try again.");
            return null;
        }
        Model validSyntax;
        try {
            validSyntax = validator.checkSyntax(fieldStream, BASE_URI, format);
        } catch (RDFParseException rpe) {
            log.warn("The dataset is not valid RDF. Error at line {}, column {}. Will fail with a bad request", rpe.getLineNumber(), rpe
                    .getColumnNumber());
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Your dataset is not valid RDF. Found an error at line " + rpe.getLineNumber() +
                    ", column " + rpe.getColumnNumber() + ". Please fix it and try again");
            return null;
        }
        return new AbstractMap.SimpleImmutableEntry<>(format, validSyntax);
    }

    /**
     * Try to find a suitable RDF format for a given file name.
     * If the part has no content type, guess the format based on the file name extension.
     * Fall back to Turtle if the guess fails, as we cannot blame the client for uploading proper content with an arbitrary (or no) extension.
     */
    private RDFFormat handleFormat(String contentType, String fileName) {
        // If the content type is not explicilty RDF, still try to guess based on the file name extension
        if (contentType == null) return Rio.getParserFormatForFileName(fileName, DEFAULT_RDF_FORMAT);
        else return Rio.getParserFormatForMIMEType(contentType, Rio.getParserFormatForFileName(fileName));
    }

    /**
     * Build a sanitized ASCII URI out of a given dataset name.
     */
    private String mintDatasetURI(String datasetName) {
        // Delete any character that is not a letter, a number, or a whitespace, as we want to mint readable URIs
        String onlyLetters = datasetName.replaceAll("[^\\p{L}\\d\\s]", "");
        // Remove diactrics to mint ASCII URIs
        String noDiacritics = Normalizer.normalize(onlyLetters, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        // Replace whitespaces with a dash
        String clean = noDiacritics.replaceAll("\\s+", "-");
        // A freshly uploaded dataset gets the "/new" state
        String datasetURI = "http://" + clean.toLowerCase(Locale.ENGLISH) + "/new";
        log.info("Named graph URI: {}", datasetURI);
        return datasetURI;
    }

    /**
     * @throws IOException if an error occurs while getting the response output writer
     */
    private void sendResponse(HttpServletResponse response, List<String> notUploaded, Map<String, List<String>> invalid, AbstractMap.SimpleImmutableEntry<Integer, List<String>>
            dataLoaderResponse)
            throws IOException {
        int dataLoaderResponseCode = dataLoaderResponse.getKey();
        List<String> dataLoaderResponseContent = dataLoaderResponse.getValue();
        // The final response code is the data loader one
        response.setStatus(dataLoaderResponseCode);
        response.setContentType("text/plain");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        try (PrintWriter pw = response.getWriter()) {
            for (String dataset : notUploaded)
                pw.println("Dataset '" + dataset + "' was not uploaded, " +
                        "since no content complied with the Wikidata RDF data model.");
            for (String dataset : invalid.keySet()) {
                List<String> invalidComponents = invalid.get(dataset);
                if (invalidComponents.isEmpty()) {
                    pw.println("Dataset '" + dataset + "' fully uploaded. Congratulations!");
                } else {
                    pw.println("Dataset '" + dataset + "' partially uploaded. The following invalid components were discarded:");
                    for (String invalidComponent : invalidComponents) pw.println(invalidComponent);
                }
            }
            if (!dataLoaderResponseContent.isEmpty()) {
                pw.println("Something went internally wrong when uploading the datasets. Reason:");
                for (String dataLoaderResponseLine : dataLoaderResponseContent) pw.println(dataLoaderResponseLine);
            }
        }
    }

    /**
     * Send the uploaded dataset to the Blazegraph bulk load service, firing a POST with the required request.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration
     * Alternative solutions may be:
     * B. use a HttpServletRequestWrapper, i.e., wrapper = new HttpServletRequestWrapper(request);
     * C. use a Filter
     *
     * @throws IOException if an input or output error is detected when the client sends the request to the data loader servlet
     */
    private AbstractMap.SimpleImmutableEntry<Integer, List<String>> sendDatasetsToDataLoader(List<File> tempDatasets, RequestParameters parameters, HttpServletResponse response) throws
            IOException {
        List<String> responseContent = new ArrayList<>();
        StringBuilder datasets = new StringBuilder();
        for (File tempDataset : tempDatasets) datasets.append(tempDataset.getPath()).append(", ");
        String fileOrDirs = "fileOrDirs";
        parameters.dataLoaderProperties.setProperty(fileOrDirs, datasets.toString());
        log.debug("Dataset files added to the Blazegraph data loader properties: {} = {}", fileOrDirs, parameters.dataLoaderProperties.getProperty(fileOrDirs));
        byte[] props;
        HttpResponse dataLoaderResponse;
        int status;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            parameters.dataLoaderProperties.store(bos, "Expected properties for the Blazegraph data loader service");
            props = bos.toByteArray();
        }
        log.debug("Final Blazegraph data loader properties: {}", parameters.dataLoaderProperties);
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                    .setScheme("http")
                    .setHost(BLAZEGRAPH_HOST)
                    .setPort(BLAZEGRAPH_PORT)
                    .setPath(BLAZEGRAPH_CONTEXT + BLAZEGRAPH_DATA_LOADER_ENDPOINT)
                    .build();
        } catch (URISyntaxException use) {
            log.error("Failed building the Blazegraph data loader URI: {}. Parse error at index {}", use.getInput(), use.getIndex());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Something went wrong while uploading the datasets. " +
                            "Reason: failed building the Blazegraph data loader URI.");
            return null;
        }
        dataLoaderResponse = Request.Post(uri)
                .bodyByteArray(props, ContentType.TEXT_PLAIN)
                .execute()
                .returnResponse();
        log.debug("Response from Blazegraph data loader: {}", dataLoaderResponse);
        status = dataLoaderResponse.getStatusLine().getStatusCode();
        // Get the data loader response only if it went wrong
        if (status == HttpServletResponse.SC_OK) {
            log.info("The datasets ingestion into Blazegraph went fine");
        } else {
            log.error("Failed ingesting one or more datasets into Blazegraph. HTTP error code: {}", status);
            try (BufferedReader responseReader = new BufferedReader(new InputStreamReader(dataLoaderResponse.getEntity().getContent(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = responseReader.readLine()) != null) {
                    responseContent.add(line);
                }
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(status, responseContent);
    }

    /**
     * Add the (dataset, uploaded by, user) and (dataset, description, description string) statements to the metadata named graph.
     *
     * @throws IOException if an input or output error is detected when the client sends the request to the SPARQL service
     */
    private boolean addMetadataQuads(RequestParameters parameters, HttpServletResponse response) throws IOException {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        String dataset = vf.createURI(parameters.datasetURI).stringValue();
        String uploadedBy = vf.createURI(UPLOADED_BY_PREDICATE).stringValue();
        String uploader = vf.createURI(USER_URI_PREFIX + parameters.user).stringValue();
        String metadataGraph = vf.createURI(METADATA_NAMESPACE).stringValue();
        StringBuilder toBeAdded = new StringBuilder("<" + dataset + "> <" + uploadedBy + "> <" + uploader + "> <" + metadataGraph + "> .");
        if (parameters.datasetDescription != null) {
            String description = vf.createURI(DESCRIPTION_PREDICATE).stringValue();
            String descriptionString = vf.createLiteral(parameters.datasetDescription).stringValue();
            String descriptionStatement = "<" + dataset + "> <" + description + "> \"" + descriptionString + "\" <" + metadataGraph + "> .";
            toBeAdded.append('\n').append(descriptionStatement);
        }
        // Fire the POST
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                    .setScheme("http")
                    .setHost(BLAZEGRAPH_HOST)
                    .setPort(BLAZEGRAPH_PORT)
                    .setPath(BLAZEGRAPH_CONTEXT + BLAZEGRAPH_SPARQL_ENDPOINT)
                    .build();
        } catch (URISyntaxException use) {
            log.error("Failed building the Blazegraph SPARQL endpoint URI: {}. Parse error at index {}", use.getInput(), use.getIndex());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Something went wrong while adding metadata that links your user name to the dataset you uploaded. " +
                            "Reason: failed building the Blazegraph SPARQL endpoint URI.");
            return false;
        }
        HttpResponse blazegraphResponse = Request.Post(uri)
                .bodyString(toBeAdded.toString(), ContentType.create("text/x-nquads"))
                .execute()
                .returnResponse();
        log.debug("Response from Blazegraph SPARQL endpoint for metadata quads: {}", blazegraphResponse);
        int status = blazegraphResponse.getStatusLine().getStatusCode();
        if (status != HttpServletResponse.SC_OK) {
            log.error("Failed sending the metadata quads to Blazegraph, got status code {}", status);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Something went wrong while adding metadata that links your user name to the dataset you uploaded. " +
                            "Reason: failed sending metadata to Blazegraph.");
            return false;
        } else {
            log.info("Successfully added metadata quads: {}", toBeAdded);
            return true;
        }
    }
}
