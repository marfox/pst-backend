package org.wikidata.query.rdf.primarysources.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 12, 2017.
 */
public class CacheUpdater implements ServletContextListener {

    private static final Logger log = LoggerFactory.getLogger(CacheUpdater.class);

    private ScheduledExecutorService entitiesService;
    private ScheduledExecutorService datasetsStatsService;

    private static ScheduledExecutorService scheduleEntitiesUpdate() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> EntitiesCache.dumpAllEntities(), Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT);
        log.info("Primary sources tool entities cache update scheduled: will run every {} {}, {} {} after the server starts.",
                Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_UNIT);
        return service;
    }

    private static ScheduledExecutorService scheduleDatasetStatsUpdate() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> DatasetsStatisticsCache.dumpStatistics(), Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT);
        log.info("Primary sources tool dataset statistics cache update scheduled: will run every {} {}, {} {} after the server starts.",
                Config.CACHE_UPDATE_INTERVAL, Config.CACHE_UPDATE_UNIT, Config.CACHE_UPDATE_INITIAL_DELAY, Config.CACHE_UPDATE_UNIT);
        return service;
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        entitiesService = scheduleEntitiesUpdate();
        datasetsStatsService = scheduleDatasetStatsUpdate();
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        entitiesService.shutdownNow();
        datasetsStatsService.shutdownNow();
    }
}
