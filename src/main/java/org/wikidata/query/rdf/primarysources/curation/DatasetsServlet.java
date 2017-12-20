package org.wikidata.query.rdf.primarysources.curation;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.IO_MIME_TYPE;
import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.runSparqlQuery;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 13, 2017.
 */
public class DatasetsServlet extends HttpServlet {

    private static final String METADATA_GRAPH = "http://www.wikidata.org/primary-sources";
    private static final String QUERY = "SELECT ?dataset ?user WHERE { GRAPH <" + METADATA_GRAPH + "> { ?user ?operation ?dataset } }";
    private static final Logger log = LoggerFactory.getLogger(DatasetsServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!request.getParameterMap().isEmpty()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No parameters accepted.");
            return;
        }
        TupleQueryResult datasetsAndUsers = runSparqlQuery(QUERY);
        sendResponse(response, datasetsAndUsers);
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult datasetsAndUsers) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray output = formatOutput(datasetsAndUsers);
        if (output == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving datasets.");
        } else if (output.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Sorry, no datasets available.");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
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
            log.error("Failed evaluating the datsets query. The stack trace follows.", qee);
            return null;
        }
        return output;
    }

}
