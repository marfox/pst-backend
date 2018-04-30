package org.wikidata.query.rdf.primarysources.common;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add the "{@code Access-Control-Allow-Origin: *}" HTTP header (AKA <i>CORS</i>) to a server response.
 * This header is needed because the Wikidata primary sources tool front end runs on <i>wikidata.org</i>,
 * while the back end is deployed on <i>pst.wmflabs.org</i>.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Apr 16, 2018.
 */
public class CorsHeaderFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(CorsHeaderFilter.class);

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.setHeader("Access-Control-Allow-Origin", "*");
        log.debug("CORS header set in response to request: {}", request);
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
