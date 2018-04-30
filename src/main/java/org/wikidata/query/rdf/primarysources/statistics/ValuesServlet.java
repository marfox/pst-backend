package org.wikidata.query.rdf.primarysources.statistics;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Get the list of available item values, i.e., values that are <i>QID</i>s.
 * <p>
 * This service is part of the Wikidata primary sources tool <i>Statistics API</i>.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Feb 27, 2018.
 */
public class ValuesServlet extends HttpServlet {

    private static final String ENTITY_TYPE = "values";
    private static final Logger log = LoggerFactory.getLogger(ValuesServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        SessionHandler sh = new SessionHandler();
        boolean ok = sh.processRequest(request, response);
        if (!ok) return;
        JSONObject entities = sh.getEntities(ENTITY_TYPE);
        if (entities == null) {
            sh.sendResponse(response, null, ENTITY_TYPE);
        } else {
            log.info("Loaded {} from cache", ENTITY_TYPE);
            sh.sendResponse(response, entities, ENTITY_TYPE);
            log.info("GET /values successful");
        }
    }

}
