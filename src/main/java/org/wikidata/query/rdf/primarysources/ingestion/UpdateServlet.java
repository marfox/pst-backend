package org.wikidata.query.rdf.primarysources.ingestion;

import java.io.BufferedReader;
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
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.openrdf.model.Model;
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

/**
 * Allow a third-party data provider to update an existing dataset.
 * This service accepts the upload of 2 files, one with the data to be removed, and one with the data to be added.
 * Both must comply with the Wikidata RDF data model.
 * See the <a href="https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model">specifications</a>.
 * <p>
 * This service is part of the Wikidata primary sources tool <i>Ingestion API</i>:
 * see <a href="https://upload.wikimedia.org/wikipedia/commons/a/a7/Wikidata_primary_sources_tool_architecture_v2.svg">this picture</a>
 * for an overview of the tool architecture.
 * <p>
 * It interacts with the Blazegraph storage engine via the
 * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29">update with multi-part request body</a>
 * service.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Jul 04, 2017.
 */
public class UpdateServlet extends HttpServlet {
    // Temporary file name of the dataset to be removed, which must be stored in the server local file system.
    private static final String TEMP_DATASET_TO_BE_REMOVED_FILE_NAME = "to_be_removed";
    // Temporary file name of the dataset to be added, which must be stored in the server local file system.
    private static final String TEMP_DATASET_TO_BE_ADDED_FILE_NAME = "to_be_added";

    private static final Logger log = LoggerFactory.getLogger(UpdateServlet.class);

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);
        /*
         * Process the multipart request
         */
        if (isMultipart) {
            ServletFileUpload upload = new ServletFileUpload();
            FileItemIterator iter;
            try {
                iter = upload.getItemIterator(request);
                while (iter.hasNext()) {
                    FileItemStream item = iter.next();
                    try (InputStream fieldStream = item.openStream()) {
                        boolean ok;
                        if (item.isFormField()) {
                            ok = handleFormField(fieldStream, item.getFieldName(), parameters, response);
                            if (!ok) return;
                        } else {
                            ok = handleFileField(item, fieldStream, parameters, response, validator);
                            if (!ok) return;
                        }
                    }
                }
                boolean checked = checkRequiredFields(parameters, response);
                if (!checked) return;
                log.debug("Required parameters stored as fields in private class: {}", parameters);
            } catch (FileUploadException fue) {
                log.error("Failed reading/parsing the request or storing files: {}", fue);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, fue.getLocalizedMessage());
                return;
            }
        } else {
            String actualContentType = request.getContentType();
            log.warn("Not a multipart content type: {} Will fail with a bad request", actualContentType);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "You should update your dataset using 'multipart/form-data' content type, not " +
                actualContentType + ". Please fix your HTTP request and try again.");
            return;
        }

        /*
         * Validate the data model
         */
        AbstractMap.SimpleImmutableEntry<Model, List<String>> validatedRemoveDataset = validator.handleDataset(parameters.removeDatasetWithValidSyntax);
        Model toBeRemoved = validatedRemoveDataset.getKey();
        Utils.addTypeToSubjectItems(toBeRemoved, parameters.targetDatasetURI.toString());
        List<String> toBeRemovedInvalid = validatedRemoveDataset.getValue();
        AbstractMap.SimpleImmutableEntry<Model, List<String>> validatedAddDataset = validator.handleDataset(parameters.addDatasetWithValidSyntax);
        Model toBeAdded = validatedAddDataset.getKey();
        Utils.addTypeToSubjectItems(toBeAdded, parameters.targetDatasetURI.toString());
        List<String> toBeAddedInvalid = validatedAddDataset.getValue();

        /*
         * Prepare the POST to the Blazegraph update service
         */
        AbstractMap.SimpleImmutableEntry<Integer, List<String>> parsedUpdateResponse;
        try {
            parsedUpdateResponse = sendUpdateToBlazegraph(parameters.targetDatasetURI, toBeRemoved, parameters.removeDatasetFormat, parameters
                    .removeDatasetFileName, toBeAdded,
                parameters.addDatasetFormat, parameters.addDatasetFileName);
        } catch (URISyntaxException use) {
            log.error("Failed building the Blazegraph update URI: {}. Parse error at index {}", use.getInput(), use.getIndex());
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Something went wrong while updating your dataset. " +
                    "Reason: failed building the Blazegraph update URI.");
            return;
        } catch (RDFHandlerException rhe) {
            log.error("Failed writing RDF: {}", rhe);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, rhe.getLocalizedMessage());
            return;
        }
        if (parsedUpdateResponse == null) {
            log.warn("No file passed the data model validation. Will fail with a 202 status code");
            response.sendError(HttpServletResponse.SC_ACCEPTED, "Neither the dataset to be removed, nor the dataset to be added has content that complies" +
                " with the Wikidata RDF data model. Nothing will be updated. Please check the documentation and try again:" +
                "https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model");
            return;
        }
        EntitiesCache.cacheDatasetEntities(parameters.targetDatasetURI.toString());
        /*
         * Build the final response
         */
        sendResponse(response, parameters.targetDatasetURI.toString(), parsedUpdateResponse, parameters.removeDatasetFileName, parameters.addDatasetFileName,
            toBeRemovedInvalid, toBeAddedInvalid);
        log.info("POST /update successful");
    }

    /**
     * Process the files uploaded by the client, firing bad requests if the formats are not recognized as RDF.
     * If a file looks like RDF, then check its syntax.
     *
     * @throws IOException if an error is detected when operating on the files
     */
    private boolean handleFileField(FileItemStream item, InputStream fieldStream, RequestParameters parameters, HttpServletResponse response,
                                    WikibaseDataModelValidator validator) throws
        IOException {
        String fieldName = item.getFieldName();
        String fileName = item.getName();
        String contentType = item.getContentType();
        switch (fieldName) {
        case ApiParameters.REMOVE_FORM_FIELD:
            log.info("Dataset to be removed file detected: {}", fileName);
            parameters.removeDatasetFileName = fileName;
            parameters.removeDatasetFormat = Utils.handleRdfFormat(contentType, fileName);
            // The part is just wrong, so fail with a bad request
            if (parameters.removeDatasetFormat == null) {
                log.warn("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                    fileName, contentType);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be removed '" + fileName +
                    "' does not match any RDF format: " + contentType + ". Please fix it and try again.");
                return false;
            }
            // Validate syntax
            try {
                parameters.removeDatasetWithValidSyntax = validator.checkSyntax(fieldStream, RdfVocabulary.BASE_URI, parameters.removeDatasetFormat);
            } catch (RDFParseException rpe) {
                log.warn("The dataset to be removed is not valid RDF. Error at line {}, column {}. Will fail with a bad request",
                    rpe.getLineNumber(), rpe.getColumnNumber());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be removed '" + fileName + "' is not valid RDF." +
                    " Found an error at line " + rpe.getLineNumber() + ", column " + rpe.getColumnNumber() + ". Please fix it and try again.");
                return false;
            }
            break;
        case ApiParameters.ADD_FORM_FIELD:
            log.info("Dataset to be added file detected: {}", fileName);
            parameters.addDatasetFileName = fileName;
            parameters.addDatasetFormat = Utils.handleRdfFormat(contentType, fileName);
            if (parameters.addDatasetFormat == null) {
                log.warn("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                    fileName, contentType);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be added '" + fileName +
                    "' does not match any RDF format: " + contentType + ". Please fix it and try again.");
                return false;
            }
            // Validate syntax
            try {
                parameters.addDatasetWithValidSyntax = validator.checkSyntax(fieldStream, RdfVocabulary.BASE_URI, parameters.addDatasetFormat);
            } catch (RDFParseException rpe) {
                log.warn("The dataset to be added is not valid RDF. Error at line {}, column {}. Will fail with a bad request",
                    rpe.getLineNumber(), rpe.getColumnNumber());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be added '" + fileName + "' is not valid RDF." +
                    " Found an error at line " + rpe.getLineNumber() + ", column " + rpe.getColumnNumber() + ". Please fix it and try again.");
                return false;
            }
            break;
        default:
            log.warn("Unexpected file field '{}' with value '{}'. Will fail with a bad request", fieldName, fileName);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected file field '" + fieldName + "' with value '" + fileName + "'");
            return false;
        }
        return true;
    }

    /**
     * Process the textual form fields given by the client, firing a bad request if there are unexpected ones.
     *
     * @throws IOException if an error is detected when reading the fields
     */
    private boolean handleFormField(InputStream stream, String fieldName, RequestParameters parameters, HttpServletResponse response) throws IOException {
        String formValue = Streams.asString(stream, StandardCharsets.UTF_8.name());
        switch (fieldName) {
        case ApiParameters.USER_NAME_PARAMETER:
            log.info("User name form field '{}' with value '{}' detected.", fieldName, formValue);
            parameters.user = formValue;
            break;
        case ApiParameters.DATASET_PARAMETER:
            log.info("Target dataset URI form field '{}' with value '{}' detected.", fieldName, formValue);
            parameters.targetDatasetURI = URI.create(formValue);
            break;
        default:
            log.warn("Unexpected form field '{}' with value '{}'. Will fail with a a bad request", fieldName, formValue);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected form field '" + fieldName + "' with value '" + formValue + "'");
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
        if (parameters.targetDatasetURI == null) {
            log.warn("No dataset URI given. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset URI given. Please use the field '" + ApiParameters.DATASET_NAME_FORM_FIELD +
                "' to send it.");
            return false;
        }
        if (parameters.removeDatasetFileName == null) {
            log.warn("No dataset file to be removed. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset file to be removed given. Please use the field '" + ApiParameters
                .REMOVE_FORM_FIELD + "' to send it.");
            return false;
        }
        if (parameters.addDatasetFileName == null) {
            log.warn("No dataset file to be added. Will fail with a bad request");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No dataset file to be added given. Please use the field '" + ApiParameters.ADD_FORM_FIELD
                + "' to send it.");
            return false;
        }
        return true;
    }

    /**
     * Build the update request, send it to the Blazegraph update with multi-part request body service.
     * The response may eventually contain messages telling the client that nothing was removed or added,
     * due to datasets not passing the Wikidata data model validation.
     *
     * @throws URISyntaxException  if the request URI is malformed
     * @throws IOException         if an error is detected when operating on temporary files
     * @throws RDFHandlerException if an error occurs when serializing RDF to the temporary files
     */
    private AbstractMap.SimpleImmutableEntry<Integer, List<String>> sendUpdateToBlazegraph(URI targetDatasetURI, Model
        toBeRemoved, RDFFormat removeDatasetFormat, String removeDatasetFileName, Model toBeAdded, RDFFormat addDatasetFormat, String addDatasetFileName)
        throws URISyntaxException, RDFHandlerException, IOException {
        File tempDatasetToBeRemoved = null;
        File tempDatasetToBeAdded = null;
        CloseableHttpClient client = HttpClients.createDefault();
        URIBuilder builder = new URIBuilder();
        URI uri;
        uri = builder
            .setScheme("http")
            .setHost(Config.HOST)
            .setPort(Config.PORT)
            .setPath(Config.CONTEXT + Config.BLAZEGRAPH_SPARQL_ENDPOINT)
            .setParameter(ApiParameters.BLAZEGRAPH_UPDATE_PARAMETER, null)
            .setParameter(ApiParameters.BLAZEGRAPH_UPDATE_DELETE_NAMED_GRAPH_PARAMETER, targetDatasetURI.toString())
            .setParameter(ApiParameters.BLAZEGRAPH_UPDATE_INSERT_NAMED_GRAPH_PARAMETER, targetDatasetURI.toString())
            .build();
        log.debug("URI built for Blazegraph update service: {}", uri);
        HttpPost post;
        post = new HttpPost(uri);
        MultipartEntityBuilder meBuilder = MultipartEntityBuilder.create();
        String nothingRemoved = null;
        String nothingAdded = null;
        if (!toBeRemoved.isEmpty()) {
            // Set a suitable extension based on the RDF format
            tempDatasetToBeRemoved = File.createTempFile(TEMP_DATASET_TO_BE_REMOVED_FILE_NAME, "." + removeDatasetFormat.getDefaultFileExtension());
            Rio.write(toBeRemoved, Files.newBufferedWriter(tempDatasetToBeRemoved.toPath(), StandardCharsets.UTF_8), removeDatasetFormat);
            log.debug("Written temporary file with dataset to be removed: {}", tempDatasetToBeRemoved);
            ContentType toBeRemovedContentType = ContentType.create(removeDatasetFormat.getDefaultMIMEType(), StandardCharsets.UTF_8);
            meBuilder.addBinaryBody(ApiParameters.REMOVE_FORM_FIELD, tempDatasetToBeRemoved, toBeRemovedContentType, TEMP_DATASET_TO_BE_REMOVED_FILE_NAME);
        } else {
            log.warn("The request succeeded, but no content in the dataset to be removed passed the data model validation. Nothing will be removed from " +
                "Blazegraph");
            nothingRemoved = "The dataset to be removed '" + removeDatasetFileName + "' has no content that complies with the Wikidata RDF data model." +
                " Nothing will be added. Please check the documentation next time: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model";
        }
        if (!toBeAdded.isEmpty()) {
            // Set a suitable extension based on the RDF format
            tempDatasetToBeAdded = File.createTempFile(TEMP_DATASET_TO_BE_ADDED_FILE_NAME, "." + addDatasetFormat.getDefaultFileExtension());
            Rio.write(toBeAdded, Files.newBufferedWriter(tempDatasetToBeAdded.toPath(), StandardCharsets.UTF_8), addDatasetFormat);
            log.debug("Written temporary file with dataset to be added: {}", tempDatasetToBeAdded);
            ContentType toBeAddedContentType = ContentType.create(addDatasetFormat.getDefaultMIMEType(), StandardCharsets.UTF_8);
            meBuilder.addBinaryBody(ApiParameters.ADD_FORM_FIELD, tempDatasetToBeAdded, toBeAddedContentType, TEMP_DATASET_TO_BE_ADDED_FILE_NAME);
        } else {
            log.warn("The request succeeded, but no content in the dataset to be added passed the data model validation. Nothing will be added to " +
                "Blazegraph");
            nothingAdded = "The dataset to be added '" + addDatasetFileName + "' has no content that complies with the Wikidata RDF data model." +
                " Nothing will be added. Please check the documentation next time: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model";
        }
        if (nothingRemoved != null && nothingAdded != null) {
            return null;
        }
        HttpEntity entity = meBuilder.build();
        post.setEntity(entity);
        log.debug("Request body that will be sent to the Blazegraph update service: {}", entity);
        try (CloseableHttpResponse updateResponse = client.execute(post)) {
            if (tempDatasetToBeRemoved != null) {
                tempDatasetToBeRemoved.delete();
                log.debug("Deleted temporary file with dataset to be removed");
            }
            if (tempDatasetToBeAdded != null) {
                log.debug("Deleted temporary file with dataset to be added");
                tempDatasetToBeAdded.delete();
            }
            return parseUpdateResponse(updateResponse, nothingRemoved, nothingAdded);
        }
    }

    /**
     * Parse the response given by the Blazegraph update with multi-part request body service.
     * Include eventual messages telling the client that nothing was removed or added.
     *
     * @throws IOException if an input or output error is detected when reading the response
     */
    private AbstractMap.SimpleImmutableEntry<Integer, List<String>> parseUpdateResponse(CloseableHttpResponse updateResponse, String nothingRemoved, String
        nothingAdded) throws IOException {
        List<String> updateResponseContent = new ArrayList<>();
        if (nothingRemoved != null) {
            updateResponseContent.add(nothingRemoved);
        }
        if (nothingAdded != null) {
            updateResponseContent.add(nothingAdded);
        }
        if (updateResponse.getStatusLine().getStatusCode() == HttpServletResponse.SC_OK) {
            log.info("The dataset update (remove + add) into Blazegraph went fine");
        } else {
            log.error("Failed updating the dataset into Blazegraph, HTTP error code: {}", updateResponse.getStatusLine().getStatusCode());
            updateResponseContent.add("Error response from Blazegraph:");
            try (BufferedReader updateResponseReader = new BufferedReader(new InputStreamReader(updateResponse.getEntity().getContent(), StandardCharsets
                .UTF_8))) {
                String line;
                while ((line = updateResponseReader.readLine()) != null) {
                    updateResponseContent.add(line);
                }
            }
        }
        return new AbstractMap.SimpleImmutableEntry<>(updateResponse.getStatusLine().getStatusCode(), updateResponseContent);
    }

    /**
     * Build the final service response, containing messages in the following order:
     * - whether nothing was removed or added, due to no content that passed the data model validation;
     * - the eventual Blazegraph response body, only if something went wrong there;
     * - the list of invalid triples of the dataset to be removed and/or added.
     *
     * @throws IOException if an error is detected when writing the response
     */
    private void sendResponse(HttpServletResponse response, String dataset, AbstractMap.SimpleImmutableEntry<Integer, List<String>> parsedUpdateResponse,
                              String removeDatasetFileName, String addDatasetFileName, List<String> toBeRemovedInvalid, List<String> toBeAddedInvalid)
        throws IOException {
        List<String> updateResponseContent = parsedUpdateResponse.getValue();
        // The final response code is the Blazegraph service one
        response.setStatus(parsedUpdateResponse.getKey());
        response.setContentType("text/plain");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        try (PrintWriter pw = response.getWriter()) {
            if (!updateResponseContent.isEmpty()) {
                for (String updateResponseLine : updateResponseContent) {
                    pw.println(updateResponseLine);
                }
            }
            if (!toBeRemovedInvalid.isEmpty()) {
                pw.println("Dataset '" + dataset + "' partially updated.");
                pw.println("The dataset to be removed '" + removeDatasetFileName + "' has the following invalid content that was discarded:");
                for (String invalidComponent : toBeRemovedInvalid) {
                    pw.println(invalidComponent);
                }
            }
            if (!toBeAddedInvalid.isEmpty()) {
                pw.println("Dataset '" + dataset + "' partially updated.");
                pw.println("The dataset to be added '" + addDatasetFileName + "' has the following invalid content that was discarded:");
                for (String invalidComponent : toBeAddedInvalid) {
                    pw.println(invalidComponent);
                }
            }
            if (updateResponseContent.isEmpty() && toBeRemovedInvalid.isEmpty() && toBeAddedInvalid.isEmpty())
                pw.println("Dataset '" + dataset + "' successfully updated.");
            pw.flush();
        }
    }

    private class RequestParameters {
        // The updater user name.
        private String user;
        // The target dataset URI to be updated.
        private URI targetDatasetURI;
        // The file name of the dataset to be removed, as given by the client.
        private String removeDatasetFileName;
        // The RDF format of the dataset to be removed.
        private RDFFormat removeDatasetFormat;
        // The RDF {@link Model} of the dataset to be removed, which has passed the syntax check.
        private Model removeDatasetWithValidSyntax;
        // The file name of the dataset to be added, as given by the client.
        private String addDatasetFileName;
        // The RDF format of the dataset to be added.
        private RDFFormat addDatasetFormat;
        // The RDF {@link Model} of the dataset to be added, which has passed the syntax check.
        private Model addDatasetWithValidSyntax;

        @Override
        public String toString() {
            return String.format("user = %s; target URI = %s; dataset to be removed: file name = %s; format = %s; valid RDF model = %s; dataset to be added: " +
                    "file name = %s; format = %s; valid RDF model = %s", user, targetDatasetURI, removeDatasetFileName, removeDatasetFormat,
                removeDatasetWithValidSyntax, addDatasetFileName, addDatasetFormat, addDatasetWithValidSyntax);
        }
    }
}
