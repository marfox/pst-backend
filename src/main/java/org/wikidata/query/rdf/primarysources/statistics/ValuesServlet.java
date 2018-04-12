package org.wikidata.query.rdf.primarysources.statistics;

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
public class ValuesServlet extends HttpServlet {

    private static final String ENTITY_TYPE = "values";
    private static final Logger log = LoggerFactory.getLogger(ValuesServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        SessionHandler sh = new SessionHandler();
        boolean ok = sh.processRequest(request, response, log);
        if (!ok) return;
        sh.sendResponse(response, sh.getEntities(ENTITY_TYPE, log), ENTITY_TYPE);
    }

}