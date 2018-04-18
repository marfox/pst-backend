package org.wikidata.query.rdf.primarysources.statistics;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Feb 27, 2018.
 */
public class PropertiesServlet extends HttpServlet {

    private static final String ENTITY_TYPE = "properties";
    private static final Logger log = LoggerFactory.getLogger(PropertiesServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        SessionHandler sh = new SessionHandler();
        boolean ok = sh.processRequest(request, response);
        if (!ok) return;
        JSONObject entities = sh.getEntities(ENTITY_TYPE);
        if (entities == null) {
            sh.sendResponse(response, entities, ENTITY_TYPE);
        } else {
            log.info("Loaded {} from cache", ENTITY_TYPE);
            sh.sendResponse(response, entities, ENTITY_TYPE);
            log.info("GET /properties successful");
        }
    }

}
