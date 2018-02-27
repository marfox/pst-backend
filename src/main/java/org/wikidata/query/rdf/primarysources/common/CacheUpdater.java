package org.wikidata.query.rdf.primarysources.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 12, 2017.
 */
public class CacheUpdater implements ServletContextListener {

    // Run once a day, 1 hour after the server starts. Change the fields below to change the schedule
    private static final TimeUnit UPDATE_UNIT = TimeUnit.valueOf(System.getenv("CACHE_UPDATE_TIME_UNIT"));
    private static final long UPDATE_INITIAL_DELAY = Long.parseLong(System.getenv("CACHE_UPDATE_INITIAL_DELAY"));
    private static final long UPDATE_INTERVAL = Long.parseLong(System.getenv("CACHE_UPDATE_INTERVAL"));

    private static final Logger log = LoggerFactory.getLogger(CacheUpdater.class);

    private ScheduledExecutorService entitiesService;
    private ScheduledExecutorService datasetsStatsService;

    private static ScheduledExecutorService scheduleEntitiesUpdate() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> EntitiesCache.dumpAllEntities(), UPDATE_INITIAL_DELAY, UPDATE_INTERVAL, UPDATE_UNIT);
        log.info("Primary sources tool entities cache update scheduled: will run every {} {}, {} {} after the server starts.",
                UPDATE_INTERVAL, UPDATE_UNIT, UPDATE_INITIAL_DELAY, UPDATE_UNIT);
        return service;
    }

    private static ScheduledExecutorService scheduleDatasetStatsUpdate() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> DatasetsStatisticsCache.dumpStatistics(), UPDATE_INITIAL_DELAY, UPDATE_INTERVAL, UPDATE_UNIT);
        log.info("Primary sources tool dataset statistics cache update scheduled: will run every {} {}, {} {} after the server starts.",
                UPDATE_INTERVAL, UPDATE_UNIT, UPDATE_INITIAL_DELAY, UPDATE_UNIT);
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
