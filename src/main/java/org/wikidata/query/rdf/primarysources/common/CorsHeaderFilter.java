package org.wikidata.query.rdf.primarysources.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Apr 16, 2018.
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
