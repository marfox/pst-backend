package org.wikidata.query.rdf.primarysources.ingestion;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.Config;
import org.wikidata.query.rdf.primarysources.common.EntitiesCache;
import org.wikidata.query.rdf.primarysources.common.RdfVocabulary;
import org.wikidata.query.rdf.primarysources.common.Utils;
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;

import com.google.common.io.Resources;

/**
 * Allow a third-party data provider to upload a dataset.
 * This service accepts one or more files, which must comply with the Wikidata RDF data model.
 * See the <a href="https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model">specifications</a>.
 * <p>
 * This service is part of the Wikidata primary sources tool <i>Ingestion API</i>:
 * see <a href="https://upload.wikimedia.org/wikipedia/commons/a/a7/Wikidata_primary_sources_tool_architecture_v2.svg">this picture</a>
 * for an overview of the tool architecture.
 * <p>
 * It interacts with the Blazegraph storage engine via the
 * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Data_Load">bulk data load</a> service.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Jul 20, 2017.
 */
public class UploadServlet extends HttpServlet {

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
            Utils.addTypeToSubjectItems(toBeUploaded, parameters.datasetURI);
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
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No user name given. Please use the field '" + ApiParameters.USER_NAME_PARAMETER + "' to " +
                "send it.");
            return false;
        }
        boolean validated = Utils.validateUserName(parameters.user);
        if (!validated) {
            log.warn("Invalid user name. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Illegal characters found in the user name: '" + parameters.user + "'. The following " +
                "characters are not allowed: : / ? # [ ] @ ! $ & ' ( ) * + , ; =");
            return false;
        }
        if (parameters.datasetFileName == null) {
            log.warn("No dataset file given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset file given. Please upload at least a file.");
            return false;
        }
        if (parameters.datasetURI == null) {
            log.warn("No dataset name given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset name given. Please use the field '" + ApiParameters.DATASET_NAME_FORM_FIELD +
                "' to send it.");
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
    private boolean handleFormField(FileItemStream item, InputStream fieldStream, RequestParameters parameters, HttpServletResponse response) throws
        IOException {
        String field = item.getFieldName();
        String value = Streams.asString(fieldStream);
        switch (field) {
        case ApiParameters.DATASET_NAME_FORM_FIELD:
            log.info("Dataset name detected. Will build a sanitized ASCII URI out of value '{}' as the named graph where the " +
                "dataset will be stored.", value);
            parameters.datasetURI = Utils.mintDatasetURI(value);
            String defaultGraph = "defaultGraph";
            parameters.dataLoaderProperties.setProperty(defaultGraph, parameters.datasetURI);
            log.debug("Named graph URI added to the Blazegraph data loader properties: {} = {}", defaultGraph, parameters.dataLoaderProperties.getProperty(
                defaultGraph));
            return true;
        case ApiParameters.DATASET_DESCRIPTION_FORM_FIELD:
            log.info("Dataset description detected. Will be stored in the metadata graph <{}>", RdfVocabulary.METADATA_NAMESPACE);
            parameters.datasetDescription = value;
            log.debug("Named graph URI added to the Blazegraph data loader properties: {} = {}", parameters.datasetDescription);
            return true;
        case ApiParameters.USER_NAME_PARAMETER:
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
                                                                               WikibaseDataModelValidator validator, HttpServletResponse response) throws
        IOException {
        String fieldName = item.getFieldName();
        String fileName = item.getName();
        String contentType = item.getContentType();
        log.info("File field '{}' with file name '{}' detected.", fieldName, fileName);
        parameters.datasetFileName = fileName;
        RDFFormat format = Utils.handleRdfFormat(contentType, fileName);
        if (format == null) {
            log.warn("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                fileName, contentType);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset '" + fileName +
                "' does not match any RDF format. Both the file content type '" + contentType + "' and the extension are invalid. Please fix this and try " +
                "again.");
            return null;
        }
        Model validSyntax;
        try {
            validSyntax = validator.checkSyntax(fieldStream, RdfVocabulary.BASE_URI, format);
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
     * @throws IOException if an error occurs while getting the response output writer
     */
    private void sendResponse(HttpServletResponse response, List<String> notUploaded, Map<String, List<String>> invalid, AbstractMap
        .SimpleImmutableEntry<Integer, List<String>>
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
    private AbstractMap.SimpleImmutableEntry<Integer, List<String>> sendDatasetsToDataLoader(List<File> tempDatasets, RequestParameters parameters,
                                                                                             HttpServletResponse response) throws
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
                .setHost(Config.HOST)
                .setPort(Config.PORT)
                .setPath(Config.CONTEXT + BLAZEGRAPH_DATA_LOADER_ENDPOINT)
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
            try (BufferedReader responseReader = new BufferedReader(new InputStreamReader(dataLoaderResponse.getEntity().getContent(), StandardCharsets
                .UTF_8))) {
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
        String uploadedBy = vf.createURI(RdfVocabulary.UPLOADED_BY_PREDICATE).stringValue();
        String uploader = vf.createURI(RdfVocabulary.USER_URI_PREFIX + parameters.user).stringValue();
        String metadataGraph = vf.createURI(RdfVocabulary.METADATA_NAMESPACE).stringValue();
        StringBuilder toBeAdded = new StringBuilder("<" + dataset + "> <" + uploadedBy + "> <" + uploader + "> <" + metadataGraph + "> .");
        if (parameters.datasetDescription != null) {
            String description = vf.createURI(RdfVocabulary.DESCRIPTION_PREDICATE).stringValue();
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
                .setHost(Config.HOST)
                .setPort(Config.PORT)
                .setPath(Config.CONTEXT + Config.BLAZEGRAPH_SPARQL_ENDPOINT)
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

    private class RequestParameters {
        private Properties dataLoaderProperties;
        private String user;
        private String datasetFileName;
        private String datasetURI;
        private String datasetDescription;

        @Override
        public String toString() {
            return String.format(
                Locale.ENGLISH,
                "user = %s; dataset file name = %s; URI = %s; description = %s; Blazegraph data loader properties = %s",
                user, datasetFileName, datasetURI, datasetDescription, dataLoaderProperties);
        }
    }
}
