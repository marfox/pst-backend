package org.wikidata.query.rdf.primarysources.common;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * A set of configuration constants and parameters used by the Wikidata primary sources tool.
 * Parameters are passed through the environment variables below.
 * <b>Integration tests need access to their values: please remember to export them before building or testing.</b>
 * <ul>
 * <li>{@code HOST}: server IP or host name, e.g., {@code 0.0.0.0} or {@code localhost};</li>
 * <li>{@code PORT}: server port, e.g., {@code 9999};</li>
 * <li>{@code CONTEXT}: server base path, e.g., {@code pst};</li>
 * <li>{@code DATASETS_CACHE}: datasets statistics cache JSON file path, e.g., {@code /home/hjfocs/datasets.json};</li>
 * <li>{@code ENTITIES_CACHE}: entities (subjects, properties, item values) cache directory, e.g., {@code /home/hjfocs/entities_cache};</li>
 * <li>{@code CACHE_UPDATE_INTERVAL}: interval between each cache update, e.g., {@code 24};</li>
 * <li>{@code CACHE_UPDATE_INITIAL_DELAY}: cache update delay after the start of the server, e.g., {@code 1}</li>
 * <li>{@code CACHE_UPDATE_TIME_UNIT}: the {@link TimeUnit} for the above values, e.g., {@code HOURS} (must be all caps).</li>
 * </ul>
 * For instance, the {@code CACHE_UPDATE_*} example values above would schedule the cache update every 24 hours, 1 hour after the server starts.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5
 * Created on Apr 17, 2018.
 */
public final class Config {

    /*
     Environment variable are needed for tests. System properties would not be read when testing.
     They should always be exported, otherwise integration tests can't run: for instance,
     when Jetty is fired up, the listener CacheUpdater looks for the ENTITIES_CACHE value.
    */
    // Blazegraph
    public static final String BLAZEGRAPH_HOST = System.getenv("HOST");
    public static final int BLAZEGRAPH_PORT = Integer.parseInt(System.getenv("PORT"));
    public static final String BLAZEGRAPH_CONTEXT = "/" + System.getenv("CONTEXT");
    public static final Path DATASETS_CACHE_PATH = Paths.get(System.getenv("DATASETS_CACHE"));
    /**
     * Endpoint name of the Blazegraph SPARQL service.
     */
    public static final String BLAZEGRAPH_SPARQL_ENDPOINT = "/sparql";
    // Cache
    static final TimeUnit CACHE_UPDATE_UNIT = TimeUnit.valueOf(System.getenv("CACHE_UPDATE_TIME_UNIT"));
    static final long CACHE_UPDATE_INITIAL_DELAY = Long.parseLong(System.getenv("CACHE_UPDATE_INITIAL_DELAY"));
    static final long CACHE_UPDATE_INTERVAL = Long.parseLong(System.getenv("CACHE_UPDATE_INTERVAL"));
    static final String ENTITIES_CACHE_DIR = System.getenv("ENTITIES_CACHE");

    private Config() {
    }
}
