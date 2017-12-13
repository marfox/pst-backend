package org.wikidata.query.rdf.primarysources.curation;

import org.json.simple.JSONArray;
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

    // Blazegraph happily ignores FILTER clauses with this query, which will get all the named graphs
    private static final String QUERY = "SELECT ?dataset WHERE { GRAPH ?dataset {} }";
    private static final Logger log = LoggerFactory.getLogger(DatasetsServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (!request.getParameterMap().isEmpty()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No parameters accepted.");
            return;
        }
        TupleQueryResult allNamedGraphs = runSparqlQuery(QUERY);
        sendResponse(response, allNamedGraphs);
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult namedGraphs) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray datasets = formatNamedGraphs(namedGraphs);
        if (datasets == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving datasets.");
        } else if (datasets.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "Sorry, no datasets available.");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                datasets.writeJSONString(pw);
            }
        }
    }

    private JSONArray formatNamedGraphs(TupleQueryResult namedGraphs) {
        JSONArray datasets = new JSONArray();
        try {
            while (namedGraphs.hasNext()) {
                String namedGraph = namedGraphs.next().getValue("dataset").stringValue();
                // Only add datasets that need curation
                if (namedGraph.endsWith("new")) datasets.add(namedGraph);
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the datsets query. The stack trace follows.", qee);
            return null;
        }
        return datasets;
    }

}
