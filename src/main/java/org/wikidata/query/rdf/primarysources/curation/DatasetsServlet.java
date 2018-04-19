package org.wikidata.query.rdf.primarysources.curation;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.RdfVocabulary;
import org.wikidata.query.rdf.primarysources.common.Utils;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 13, 2017.
 */
public class DatasetsServlet extends HttpServlet {

    private static final String QUERY = "SELECT ?dataset ?user WHERE { GRAPH <" + RdfVocabulary.METADATA_NAMESPACE + "> { ?dataset <" + RdfVocabulary
        .UPLOADED_BY_PREDICATE + "> ?user } }";
    private static final Logger log = LoggerFactory.getLogger(DatasetsServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Map<String, String[]> parameterMap = request.getParameterMap();
        if (!parameterMap.isEmpty()) {
            log.warn("Request parameters detected: {}. Will fail with a bad request", parameterMap);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No parameters accepted.");
            return;
        }
        TupleQueryResult datasetsAndUsers = Utils.runSparqlQuery(QUERY);
        sendResponse(response, datasetsAndUsers);
        log.info("GET /datasets successful");
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult datasetsAndUsers) throws IOException {
        JSONArray output = formatOutput(datasetsAndUsers);
        if (output == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving datasets.");
        } else if (output.isEmpty()) {
            log.warn("No datasets available, will fail with a not found");
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Sorry, no datasets available.");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                output.writeJSONString(pw);
            }
        }
    }

    private JSONArray formatOutput(TupleQueryResult datasetsAndUsers) {
        JSONArray output = new JSONArray();
        try {
            while (datasetsAndUsers.hasNext()) {
                JSONObject datasetAndUser = new JSONObject();
                BindingSet result = datasetsAndUsers.next();
                String dataset = result.getValue("dataset").stringValue();
                String user = result.getValue("user").stringValue();
                datasetAndUser.put("dataset", dataset);
                datasetAndUser.put("user", user);
                output.add(datasetAndUser);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the datasets query. The stack trace follows.", qee);
            return null;
        }
        log.debug("JSON response body to be sent to the client: {}", output);
        return output;
    }

}
