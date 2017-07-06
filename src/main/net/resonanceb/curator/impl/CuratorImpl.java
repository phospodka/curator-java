package net.resonanceb.curator.impl;

import net.resonanceb.curator.Curator;
import net.resonanceb.curator.config.ElasticClient;
import net.resonanceb.curator.config.Settings;
import net.resonanceb.curator.core.IndexOptions;
import net.resonanceb.curator.core.Operations;
import net.resonanceb.curator.core.impl.OperationsImpl;

import org.elasticsearch.client.Client;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CuratorImpl implements Curator {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    // deciding on how I want to use Spring in this; probably separate out this into a module and keep the core for lightweight
    private ElasticClient elasticClient = new ElasticClient();

    private Operations operations = new OperationsImpl();

    @Override
    public void close() {
        IndexOptions options = new IndexOptions();
        options.setIncludeClosed(false);
        options.setIncludeOpen(true);
        options.setCutoff(LocalDate.now().minusDays(Settings.closeCutoff));

        try (Client client = elasticClient.getClient()) {
            for (String prefix : Settings.prefixes) {
                options.setPrefix(prefix);

                for (String index : operations.findAllIndices(options, client)) {
                    try {
                        operations.closeIndex(index, client);
                    } catch (IOException e) {
                        LOGGER.error("Error closing index: " + index, e);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error finding indices", e);
        }
    }

    @Override
    public void delete() {
        IndexOptions options = new IndexOptions();
        options.setIncludeClosed(true);
        options.setIncludeOpen(true);
        options.setCutoff(LocalDate.now().minusDays(Settings.deleteCutoff));

        try (Client client = elasticClient.getClient()) {
            for (String prefix : Settings.prefixes) {
                options.setPrefix(prefix);

                for (String index : operations.findAllIndices(options, client)) {
                    try {
                        operations.deleteIndex(index, client);
                    } catch (IOException e) {
                        LOGGER.error("Error deleting index: " + index, e);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error finding indices", e);
        }
    }

    @Override
    public void backup() {
        IndexOptions options = new IndexOptions();
        options.setIncludeClosed(false);
        options.setIncludeOpen(true);
        options.setCutoff(LocalDate.now().minusDays(Settings.backupCutoff));

        try (Client client = elasticClient.getClient()) {
            for (String prefix : Settings.prefixes) {
                options.setPrefix(prefix);

                for (String index : operations.findAllIndices(options, client)) {
                    try {
                        operations.createSnapshot(index, client);
                    } catch (IOException e) {
                        LOGGER.error("Error backing up index: " + index, e);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error finding indices", e);
        }
    }

    @Override
    public void forceMerge() {
        IndexOptions options = new IndexOptions();
        options.setIncludeClosed(false);
        options.setIncludeOpen(true);
        options.setCutoff(LocalDate.now().minusDays(Settings.forceMergeCutoff));

        try (Client client = elasticClient.getClient()) {
            for (String prefix : Settings.prefixes) {
                options.setPrefix(prefix);

                for (String index : operations.findAllIndices(options, client)) {
                    try {
                        operations.forceMergeIndex(index, Settings.maxSegments, client);
                    } catch (IOException e) {
                        LOGGER.error("Error force merging index: " + index, e);
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error finding indices", e);
        }
    }
}
