package org.wikidata.query.rdf.primarysources.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Apr 17, 2018.
 */
public final class Config {

    /**
     * Endpoint name of the Blazegraph SPARQL service.
     */
    public static final String BLAZEGRAPH_SPARQL_ENDPOINT = "/sparql";
    /* Environment variables */
    // Blazegraph
    public static final String BLAZEGRAPH_HOST = System.getenv("HOST");
    public static final int BLAZEGRAPH_PORT = Integer.parseInt(System.getenv("PORT"));
    public static final String BLAZEGRAPH_CONTEXT = "/" + System.getenv("CONTEXT");
    public static final Path DATASETS_CACHE_PATH = Paths.get(System.getenv("DATASETS_CACHE"));
    // Cache
    static final TimeUnit CACHE_UPDATE_UNIT = TimeUnit.valueOf(System.getenv("CACHE_UPDATE_TIME_UNIT"));
    static final long CACHE_UPDATE_INITIAL_DELAY = Long.parseLong(System.getenv("CACHE_UPDATE_INITIAL_DELAY"));
    static final long CACHE_UPDATE_INTERVAL = Long.parseLong(System.getenv("CACHE_UPDATE_INTERVAL"));
    /*
     An environment variable is needed for tests. A system property would not be read when testing.
     IMPORTANT: ENTITIES_CACHE and DATASETS_CACHE should always be exported, otherwise integration tests can't run:
     when Jetty is fired up, the listener CacheUpdater looks for that variable.
    */
    static final String ENTITIES_CACHE_DIR = System.getenv("ENTITIES_CACHE");

    private Config() {
    }
}
