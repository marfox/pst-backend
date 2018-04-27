package org.wikidata.query.rdf.primarysources.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A scheduler to periodically update the entity and datasets statistics cache files.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5
 * Created on Dec 12, 2017.
 */
public class CacheUpdater implements ServletContextListener {

    private static final Logger log = LoggerFactory.getLogger(CacheUpdater.class);

    private ScheduledExecutorService entitiesService;
    private ScheduledExecutorService datasetsStatsService;

    private static ScheduledExecutorService scheduleEntitiesUpdate() {
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("entities-cache-update-scheduler-%d").build();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(tf);
        service.scheduleAtFixedRate(EntitiesCache::dumpAllEntities, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_INTERVAL, Config
            .CACHE_UPDATE_UNIT);
        log.info("Primary sources tool entities cache update scheduled: will run every {} {}, {} {} after the server starts.",
            Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_UNIT);
        return service;
    }

    private static ScheduledExecutorService scheduleDatasetsStatsUpdate() {
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("datasets-statistics-cache-update-scheduler-%d").build();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(tf);
        service.scheduleAtFixedRate(DatasetsStatisticsCache::dumpStatistics, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_INTERVAL, Config
            .CACHE_UPDATE_UNIT);
        log.info("Primary sources tool dataset statistics cache update scheduled: will run every {} {}, {} {} after the server starts.",
            Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_UNIT);
        return service;
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        entitiesService = scheduleEntitiesUpdate();
        datasetsStatsService = scheduleDatasetsStatsUpdate();
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        entitiesService.shutdownNow();
        datasetsStatsService.shutdownNow();
    }
}
