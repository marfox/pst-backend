package org.wikidata.query.rdf.primarysources.ingestion;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
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
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;
import org.wikidata.query.rdf.primarysources.common.SubjectsCache;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.*;

/**
 * This Web service allows a third-party data provider to update a given dataset.
 * It accepts the upload of 2 files, one with the data to be removed, and one with the data to be added.
 * <p>
 * The service is part of the Wikidata primary sources tool Ingestion API:
 * see <a href="https://www.wikidata.org/wiki/Wikidata:Primary_sources_tool">this document</a> for details on the tool architecture.
 * <p>
 * It consumes the Blazegraph API
 * <a href="https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29">update with multi-part request body</a>
 * service.
 *
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Jul 20, 2017.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class UpdateServlet extends HttpServlet {
    /**
     * Required HTML form field: the value should be the URI of the target dataset to be updated.
     */
    static final String TARGET_DATASET_URI_FORM_FIELD = "dataset";
    /**
     * Required HTML form field: the value should be the file with the dataset to be removed.
     */
    static final String REMOVE_FORM_FIELD = "remove";
    /**
     * Required HTML form field: the value should be the file with the dataset to be added.
     */
    static final String ADD_FORM_FIELD = "add";
    /**
     * Temporary file name of the dataset to be removed, which must be stored in the server local file system.
     */
    private static final String TEMP_DATASET_TO_BE_REMOVED_FILE_NAME = "to_be_removed";
    /**
     * Temporary file name of the dataset to be added, which must be stored in the server local file system.
     */
    private static final String TEMP_DATASET_TO_BE_ADDED_FILE_NAME = "to_be_added";
    /**
     * Query parameter (with no value) to call the Blazegraph update with multi-part request body service.
     * See https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     */
    private static final String BLAZEGRAPH_UPDATE_PARAMETER = "updatePost";
    /**
     * Query parameter for the Blazegraph update service: the value should be the URI of the target dataset that undergoes deletion.
     * This is not documented in https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     * but can be found in the source code:
     * https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L896
     */
    private static final String BLAZEGRAPH_UPDATE_DELETE_NAMED_GRAPH_PARAMETER = "context-uri-delete";
    /**
     * Query parameter for the Blazegraph update service: the value should be the URI of the target dataset that undergoes addition.
     * This is not documented in https://wiki.blazegraph.com/wiki/index.php/REST_API#UPDATE_.28POST_with_Multi-Part_Request_Body.29
     * but can be found in the source code:
     * https://github.com/blazegraph/database/blob/master/bigdata-core/bigdata-sails/src/java/com/bigdata/rdf/sail/webapp/UpdateServlet.java#L877
     */
    private static final String BLAZEGRAPH_UPDATE_INSERT_NAMED_GRAPH_PARAMETER = "context-uri-insert";

    private static final Logger log = LoggerFactory.getLogger(UpdateServlet.class);

    /**
     * The client user name.
     */
    private String user;
    /**
     * The target dataset URI to be updated.
     */
    private URI targetDatasetURI;
    /**
     * The file name of the dataset to be removed, as given by the client.
     */
    private String removeDatasetFileName;
    /**
     * The RDF format of the dataset to be removed.
     */
    private RDFFormat removeDatasetFormat;
    /**
     * The RDF {@link Model} of the dataset to be removed, which has passed the syntax check.
     */
    private Model removeDatasetWithValidSyntax;
    /**
     * The file name of the dataset to be added, as given by the client.
     */
    private String addDatasetFileName;
    /**
     * The RDF format of the dataset to be added.
     */
    private RDFFormat addDatasetFormat;
    /**
     * The RDF {@link Model} of the dataset to be added, which has passed the syntax check.
     */
    private Model addDatasetWithValidSyntax;

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
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
                            ok = handleFormField(fieldStream, item.getFieldName(), response);
                            if (!ok) {
                                return;
                            }
                        } else {
                            ok = handleFileField(item, fieldStream, response, validator);
                            if (!ok) {
                                return;
                            }
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
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "You should update your dataset using 'multipart/form-data' content type, not " +
                    actualContentType + ". Please fix your HTTP request and try again.");
            return;
        }

        /*
         * Validate the data model
         */
        AbstractMap.SimpleImmutableEntry<Model, List<String>> validatedRemoveDataset = validator.handleDataset(removeDatasetWithValidSyntax);
        Model toBeRemoved = validatedRemoveDataset.getKey();
        List<String> toBeRemovedInvalid = validatedRemoveDataset.getValue();
        AbstractMap.SimpleImmutableEntry<Model, List<String>> validatedAddDataset = validator.handleDataset(addDatasetWithValidSyntax);
        Model toBeAdded = validatedAddDataset.getKey();
        List<String> toBeAddedInvalid = validatedAddDataset.getValue();

        /*
         * Prepare the POST to the Blazegraph update service
         */
        AbstractMap.SimpleImmutableEntry<Integer, List<String>> parsedUpdateResponse;
        try {
            parsedUpdateResponse = sendUpdateToBlazegraph(targetDatasetURI, toBeRemoved, removeDatasetFormat, removeDatasetFileName, toBeAdded,
                    addDatasetFormat, addDatasetFileName);
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
            response.sendError(HttpServletResponse.SC_ACCEPTED, "Neither the dataset to be removed, nor the dataset to be added has content that complies" +
                    " with the Wikidata RDF data model. Nothing will be updated. Please check the documentation and try again:" +
                    "https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model");
            return;
        }
        SubjectsCache.cacheDatasetSubjects(targetDatasetURI.toString());
        /*
         * Build the final response
         */
        respond(response, removeDatasetFileName, addDatasetFileName, toBeRemovedInvalid, toBeAddedInvalid, parsedUpdateResponse);
    }

    /**
     * Process the files uploaded by the client, firing bad requests if the formats are not recognized as RDF.
     * If a file looks like RDF, then check its syntax.
     *
     * @throws IOException if an error is detected when operating on the files
     */
    private boolean handleFileField(FileItemStream item, InputStream fieldStream, HttpServletResponse response, WikibaseDataModelValidator validator) throws
            IOException {
        String fieldName = item.getFieldName();
        String fileName = item.getName();
        String contentType = item.getContentType();
        switch (fieldName) {
            case REMOVE_FORM_FIELD:
                log.info("Dataset to be removed file field '{}' with value '{}' detected.", fieldName, fileName);
                removeDatasetFileName = fileName;
                removeDatasetFormat = handleFormat(contentType, fileName);
                // The part is just wrong, so fail with a bad request
                if (removeDatasetFormat == null) {
                    log.error("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                            fileName, contentType);
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be removed '" + fileName +
                            "' does not match any RDF format: " + contentType + ". Please fix it and try again.");
                    return false;
                }
                // Validate syntax
                try {
                    removeDatasetWithValidSyntax = validator.checkSyntax(fieldStream, UploadServlet.BASE_URI, removeDatasetFormat);
                } catch (RDFParseException rpe) {
                    log.error("The dataset to be removed is not valid RDF. Error at line {}, column {}. Will fail with a bad request",
                            rpe.getLineNumber(), rpe.getColumnNumber());
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be removed '" + fileName + "' is not valid RDF." +
                            " Found an error at line " + rpe.getLineNumber() + ", column " + rpe.getColumnNumber() + ". Please fix it and try again.");
                    return false;
                }
                break;
            case ADD_FORM_FIELD:
                log.info("Dataset to be added file field '{}' with value '{}' detected.", fieldName, fileName);
                addDatasetFileName = fileName;
                addDatasetFormat = handleFormat(contentType, fileName);
                if (addDatasetFormat == null) {
                    log.error("Both the content type and the extension are invalid for file '{}': {}. Will fail with a bad request",
                            fileName, contentType);
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be added '" + fileName +
                            "' does not match any RDF format: " + contentType + ". Please fix it and try again.");
                    return false;
                }
                // Validate syntax
                try {
                    addDatasetWithValidSyntax = validator.checkSyntax(fieldStream, UploadServlet.BASE_URI, addDatasetFormat);
                } catch (RDFParseException rpe) {
                    log.error("The dataset to be added is not valid RDF. Error at line {}, column {}. Will fail with a bad request",
                            rpe.getLineNumber(), rpe.getColumnNumber());
                    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The dataset to be added '" + fileName + "' is not valid RDF." +
                            " Found an error at line " + rpe.getLineNumber() + ", column " + rpe.getColumnNumber() + ". Please fix it and try again.");
                    return false;
                }
                break;
            default:
                log.error("Unexpected file field '{}' with value '{}'. Will fail with a bad request", fieldName, fileName);
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
    private boolean handleFormField(InputStream stream, String fieldName, HttpServletResponse response) throws IOException {
        String formValue = Streams.asString(stream, StandardCharsets.UTF_8.name());
        switch (fieldName) {
            case UploadServlet.USER_NAME_FORM_FIELD:
                log.info("User name form field '{}' with value '{}' detected.", fieldName, formValue);
                user = formValue;
                break;
            case TARGET_DATASET_URI_FORM_FIELD:
                log.info("Target dataset URI form field '{}' with value '{}' detected.", fieldName, formValue);
                targetDatasetURI = URI.create(formValue);
                break;
            default:
                log.error("Unexpected form field '{}' with value '{}'. Will fail with a a bad request", fieldName, formValue);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected form field '" + fieldName + "' with value '" + formValue + "'");
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
                .setHost(BLAZEGRAPH_HOST)
                .setPort(BLAZEGRAPH_PORT)
                .setPath(BLAZEGRAPH_CONTEXT + BLAZEGRAPH_SPARQL_ENDPOINT)
                .setParameter(BLAZEGRAPH_UPDATE_PARAMETER, null)
                .setParameter(BLAZEGRAPH_UPDATE_DELETE_NAMED_GRAPH_PARAMETER, targetDatasetURI.toString())
                .setParameter(BLAZEGRAPH_UPDATE_INSERT_NAMED_GRAPH_PARAMETER, targetDatasetURI.toString())
                .build();
        HttpPost post;
        post = new HttpPost(uri);
        MultipartEntityBuilder meBuilder = MultipartEntityBuilder.create();
        String nothingRemoved = null;
        String nothingAdded = null;
        if (!toBeRemoved.isEmpty()) {
            // Set a suitable extension based on the RDF format
            tempDatasetToBeRemoved = File.createTempFile(TEMP_DATASET_TO_BE_REMOVED_FILE_NAME, "." + removeDatasetFormat.getDefaultFileExtension());
            Rio.write(toBeRemoved, Files.newBufferedWriter(tempDatasetToBeRemoved.toPath(), StandardCharsets.UTF_8), removeDatasetFormat);
            ContentType toBeRemovedContentType = ContentType.create(removeDatasetFormat.getDefaultMIMEType(), StandardCharsets.UTF_8);
            meBuilder.addBinaryBody(REMOVE_FORM_FIELD, tempDatasetToBeRemoved, toBeRemovedContentType, TEMP_DATASET_TO_BE_REMOVED_FILE_NAME);
        } else {
            log.warn("The request succeeded, but no content in the dataset to be removed passed the data model validation. Nothing will be removed from " +
                    "Blazegraph");
            nothingRemoved = "The dataset to be removed '" + removeDatasetFileName + "' has no content that complies with the Wikidata RDF data model." +
                    "Nothing will be added. Please check the documentation next time: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model";
        }
        if (!toBeAdded.isEmpty()) {
            // Set a suitable extension based on the RDF format
            tempDatasetToBeAdded = File.createTempFile(TEMP_DATASET_TO_BE_ADDED_FILE_NAME, "." + addDatasetFormat.getDefaultFileExtension());
            Rio.write(toBeAdded, Files.newBufferedWriter(tempDatasetToBeAdded.toPath(), StandardCharsets.UTF_8), addDatasetFormat);
            ContentType toBeAddedContentType = ContentType.create(addDatasetFormat.getDefaultMIMEType(), StandardCharsets.UTF_8);
            meBuilder.addBinaryBody(ADD_FORM_FIELD, tempDatasetToBeAdded, toBeAddedContentType, TEMP_DATASET_TO_BE_ADDED_FILE_NAME);
        } else {
            log.warn("The request succeeded, but no content in the dataset to be added passed the data model validation. Nothing will be added to " +
                    "Blazegraph");
            nothingAdded = "The dataset to be added '" + addDatasetFileName + "' has no content that complies with the Wikidata RDF data model." +
                    "Nothing will be added. Please check the documentation next time: https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model";
        }
        if (nothingRemoved != null && nothingAdded != null) {
            return null;
        }
        post.setEntity(meBuilder.build());
        try (CloseableHttpResponse updateResponse = client.execute(post)) {
            if (tempDatasetToBeRemoved != null) {
                tempDatasetToBeRemoved.delete();
            }
            if (tempDatasetToBeAdded != null) {
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
            log.info("The dataset upload (remove + add) into Blazegraph went fine");
        } else {
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
     * Try to find a suitable RDF format for a given file name.
     * If the part has no content type, guess the format based on the file name extension.
     * Fall back to Turtle if the guess fails, as we cannot blame the client for uploading proper content with an arbitrary (or no) extension.
     */
    private RDFFormat handleFormat(String contentType, String fileName) {
        if (contentType == null) {
            return Rio.getParserFormatForFileName(fileName, UploadServlet.DEFAULT_RDF_FORMAT);
        } else {
            // If the content type is not explicilty RDF, still try to guess based on the file name extension
            return Rio.getParserFormatForMIMEType(contentType, Rio.getParserFormatForFileName(fileName));
        }
    }

    /**
     * Build the final service response, containing messages in the following order:
     * - whether nothing was removed or added, due to no content that passed the data model validation;
     * - the eventual Blazegraph response body, only if something went wrong there;
     * - the list of invalid triples of the dataset to be removed and/or added.
     *
     * @throws IOException if an error is detected when writing the response
     */
    private void respond(HttpServletResponse response, String removeDatasetFileName, String addDatasetFileName, List<String> toBeRemovedInvalid, List<String>
            toBeAddedInvalid, AbstractMap.SimpleImmutableEntry<Integer, List<String>> parsedUpdateResponse) throws IOException {
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
                pw.println("Dataset to be removed '" + removeDatasetFileName + "', list of invalid components:");
                for (String invalidComponent : toBeRemovedInvalid) {
                    pw.println(invalidComponent);
                }
            }
            if (!toBeAddedInvalid.isEmpty()) {
                pw.println("Dataset to be added '" + addDatasetFileName + "', list of invalid components:");
                for (String invalidComponent : toBeAddedInvalid) {
                    pw.println(invalidComponent);
                }
            }
            pw.flush();
        }
    }
}
