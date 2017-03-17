package net.resonanceb.curator.impl;

import net.resonanceb.curator.Operations;
import net.resonanceb.curator.Options;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotNull;

public class OperationsImpl implements Operations {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private static final String DATE_SEPARATOR = ".";
    private static final String REPOSITORY_LOCATION = "/tmp/elasticsearch/archive";
    private static final String REPOSITORY_NAME = "elastic_repository";
    private static final String REPOSITORY_TYPE = "fs";
    private static final String SNAPSHOT_PREFIX = "snapshot_";
    private static final String WILDCARD = "*";

    @Override
    public void createSnapshot(@NotNull String index, @NotNull Client client) throws IOException {
        if(!isIndexClosed(index, client)) {
            LOGGER.debug("Creating snapshot for index:{}", index);

            String snapshotName = SNAPSHOT_PREFIX + index;

            // Check if the snapshot exists before attempting to create it
            // todo work more with incremental snapshot stuff to understand this better
            if (!isSnapshotExists(client, snapshotName)) {

                CreateSnapshotRequest request = CreateSnapshotAction.INSTANCE.newRequestBuilder(client)
                        .setRepository(REPOSITORY_NAME)
                        .setIndices(index)
                        .setIncludeGlobalState(false)
                        .setWaitForCompletion(true)
                        .setSnapshot(snapshotName)
                        .request();

                client.admin().cluster().createSnapshot(request).actionGet();
            } else {
                LOGGER.debug("Snapshot name:{} is already in use", snapshotName);
            }
        } else {
            LOGGER.debug("Attempting to backup index:{} but it is already closed", index);
        }
    }

    @Override
    public void deleteSnapshot(String snapshotName, Client client) {
        try {
            LOGGER.debug("Deleting snapshot:{}", snapshotName);

            DeleteSnapshotRequest deleteRequest = DeleteSnapshotAction.INSTANCE.newRequestBuilder(client)
                    .setRepository(REPOSITORY_NAME)
                    .setSnapshot(snapshotName)
                    .request();

            client.admin().cluster().deleteSnapshot(deleteRequest).actionGet();
        } catch(SnapshotMissingException e) {
            LOGGER.debug("Snapshot name:{} was not found for deletion", snapshotName);
        }
    }

    @Override
    public GetSnapshotsResponse getSnapshot(String snapshotName, Client client) throws IOException {
        LOGGER.debug("Get repository:{}", REPOSITORY_NAME);

        GetSnapshotsRequest getRequest = GetSnapshotsAction.INSTANCE.newRequestBuilder(client)
                .setRepository(REPOSITORY_NAME)
                .setSnapshots(snapshotName)
                .request();

        return client.admin().cluster().getSnapshots(getRequest).actionGet();
    }

    @Override
    public void createRepository(Client client) throws IOException {
        LOGGER.debug("Creating repository:{}", REPOSITORY_NAME);
        PutRepositoryRequest putRequest = PutRepositoryAction.INSTANCE.newRequestBuilder(client)
                .setName(REPOSITORY_NAME)
                .setType(REPOSITORY_TYPE)
                .setSettings(Settings.settingsBuilder()
                        .put("compress", true)
                        .put("location", REPOSITORY_LOCATION)
                        .build()
                )
                .request();

        client.admin().cluster().putRepository(putRequest).actionGet();
    }

    @Override
    public GetRepositoriesResponse getRepository(Client client) throws IOException {
        LOGGER.debug("Get repository:{}", REPOSITORY_NAME);

        GetRepositoriesRequest getRequest = GetRepositoriesAction.INSTANCE.newRequestBuilder(client)
                .setRepositories(REPOSITORY_NAME)
                .request();

        return client.admin().cluster().getRepositories(getRequest).actionGet();
    }

    @Override
    public void closeIndex(@NotNull String index, @NotNull Client client) throws IOException {
        if(!isIndexClosed(index ,client)) {

            LOGGER.debug("Closing index:{}", index);

            CloseIndexRequest request = CloseIndexAction.INSTANCE.newRequestBuilder(client)
                    .setIndices(index)
                    .request();

            client.admin().indices().close(request).actionGet();

        } else {
            LOGGER.debug("Attempting to close index:{} but it is already closed", index);
        }
    }

    @Override
    public void deleteIndex(@NotNull String index, @NotNull Client client) throws IOException {
        LOGGER.debug("Deleting index:{}", index);

        client.admin().indices().delete(
                new DeleteIndexRequest().indices(index)
        ).actionGet();
    }

    @Override
    public void openIndex(@NotNull String index, @NotNull Client client) throws IOException {
        LOGGER.debug("Opening index:{}", index);

        OpenIndexRequest request = OpenIndexAction.INSTANCE.newRequestBuilder(client)
                .setIndices(index)
                .request();

        client.admin().indices().open(request).actionGet();
    }

    @Override
    public void forceMergeIndex(@NotNull String index, int maxSegments, @NotNull Client client) throws IOException {
        if(!isIndexClosed(index, client)) {
            LOGGER.debug("Merging segments of index:{}", index);

            // [number of shards, number of segments]
            int[] counts = getSegmentCount(index, client);
            int shardCount = counts[0];
            int segmentCount = counts[1];

            if(segmentCount > (shardCount * maxSegments)) {
                ForceMergeRequest request = ForceMergeAction.INSTANCE.newRequestBuilder(client)
                        .setIndices(index)
                        .setMaxNumSegments(maxSegments)
                        .request();

                client.admin().indices().forceMerge(request).actionGet();
            }
        } else {
            LOGGER.debug("Attempting to merge index:{} but it is already closed", index);
        }
    }

    @Override
    public List<String> findAllIndices(@NotNull Options options, @NotNull Client client) throws IOException {
        LOGGER.debug("Finding all indices using options:{}", options);
        String[] indices = getIndices(client, options.getPrefix(), options.isIncludeOpen(), options.isIncludeClosed());

        List<String> expiredIndices = new ArrayList<>();

        for(String index : indices) {
            String suffix = index.substring(options.getPrefix().length());
            LocalDate indexTime = getIndexDate(suffix);

            if(indexTime.isBefore(options.getCutoff())) {
                expiredIndices.add(index);
            }
        }

        return expiredIndices;
    }

    /**
     * Parse the suffix of an index into a date object
     * todo plan to use java LocalDate instead of the builder options are super different
     * @param indexSuffix the part of the index that holds the date string
     * @return {@link LocalDate}
     */
    private LocalDate getIndexDate(String indexSuffix) {
        // Date in the format YYYY.MM.DD which is the default logstash format
        DateTimeFormatter format = new DateTimeFormatterBuilder()
                .appendYear(4, 4)
                .appendLiteral(DATE_SEPARATOR)
                .appendMonthOfYear(2)
                .appendLiteral(DATE_SEPARATOR)
                .appendDayOfMonth(2)
                .toFormatter();

        return LocalDate.parse(indexSuffix, format);
    }

    /**
     * Get the list of indices matching the prefix and open / close options.
     * @param client {@link Client} that can connect to elasticsearch
     * @param prefix of indices to search for
     * @param includeOpen boolean flag to include open indices
     * @param includeClosed boolean flag to include closed indices
     * @return Array of index names
     * @throws IOException
     */
    private String[] getIndices(Client client, String prefix, boolean includeOpen, boolean includeClosed) throws IOException {
        GetSettingsRequest request = GetSettingsAction.INSTANCE.newRequestBuilder(client)
                .setIndices(prefix + WILDCARD)
                .setIndicesOptions(IndicesOptions.fromOptions(false, false, includeOpen, includeClosed))
                .request();

        GetSettingsResponse response = client.admin().indices().getSettings(request).actionGet();

        return response.getIndexToSettings().keys().toArray(String.class);
    }

    /**
     * Get the total number of segments for the requested index.
     * @param index name to query
     * @param client {@link Client} for elasticsearch
     * @return Array [number of shards, number of segments]
     */
    private int[] getSegmentCount(String index, Client client) {
        IndicesSegmentsRequest request = IndicesSegmentsAction.INSTANCE.newRequestBuilder(client)
                .setIndices(index)
                .request();

        IndicesSegmentResponse response = client.admin().indices().segments(request).actionGet();
        Map<String, IndexSegments> indexSegments = response.getIndices();

        int segmentCount = 0;
        int shardCount = 0;

        // Navigate the layers to get the total number of segments across all shards of the index
        for(Map.Entry<String, IndexSegments> indexEntry : indexSegments.entrySet()) {
            IndexSegments indexSegment = indexEntry.getValue();

            Map<Integer, IndexShardSegments> indexShardSegments = indexSegment.getShards();

            for(Map.Entry<Integer, IndexShardSegments> indexShardEntry : indexShardSegments.entrySet()) {
                IndexShardSegments indexShardSegment = indexShardEntry.getValue();

                ShardSegments[] shardSegments = indexShardSegment.getShards();

                for(ShardSegments shardSegment : shardSegments) {
                    ++shardCount;
                    segmentCount += shardSegment.getSegments().size();
                }
            }
        }

        return new int[]{shardCount, segmentCount};
    }

    /**
     * Check if the index is closed
     * todo I think this is no longer needed with the right options; may still be a useful operation to have in the bucket though
     * @param index to query
     * @param client {@link Client} for elasticsearch
     * @return boolean if index is closed
     * @throws IOException
     */
    protected boolean isIndexClosed(String index, Client client) throws IOException {
        ClusterStateRequest request = ClusterStateAction.INSTANCE.newRequestBuilder(client)
                .setIndices(index)
                .request();

        ClusterStateResponse response = client.admin().cluster().state(request).actionGet();

        return response.getState().getMetaData().getIndices().get(index).getState().name().equalsIgnoreCase("close");
    }

    /**
     * Check if the snapshot exists for the index.
     * @param client {@link Client} that can connect to elasticsearch
     * @param snapshotName name of snapshot to lookup
     * @return boolean whether snapshot exists
     */
    protected boolean isSnapshotExists(Client client, String snapshotName) {
        boolean exists = true;

        try {
            GetSnapshotsRequest getRequest = GetSnapshotsAction.INSTANCE.newRequestBuilder(client)
                    .setRepository(REPOSITORY_NAME)
                    .setSnapshots(snapshotName)
                    .request();

            client.admin().cluster().getSnapshots(getRequest).actionGet();
        } catch(SnapshotMissingException e) {
            exists = false;
            LOGGER.debug("Snapshot name:{} is available to be created", snapshotName);
        }

        return exists;
    }
}
