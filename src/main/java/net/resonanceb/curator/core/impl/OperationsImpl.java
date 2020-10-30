package net.resonanceb.curator.core.impl;

import net.resonanceb.curator.core.IndexOptions;
import net.resonanceb.curator.core.Operations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    public void createSnapshot(@NotNull String index, @NotNull RestHighLevelClient client) throws IOException {
        if(!isIndexClosed(index, client)) {
            LOGGER.debug("Creating snapshot for index:{}", index);

            String snapshotName = SNAPSHOT_PREFIX + index;

            // Check if the snapshot exists before attempting to create it
            // todo work more with incremental snapshot stuff to understand this better
            if (!isSnapshotExists(client, snapshotName)) {

                CreateSnapshotRequest request = new CreateSnapshotRequest()
                        .repository(REPOSITORY_NAME)
                        .indices(index)
                        .includeGlobalState(false)
                        .waitForCompletion(true)
                        .snapshot(snapshotName);

                client.snapshot().create(request, RequestOptions.DEFAULT);
            } else {
                LOGGER.debug("Snapshot name:{} is already in use", snapshotName);
            }
        } else {
            LOGGER.debug("Attempting to backup index:{} but it is already closed", index);
        }
    }

    @Override
    public void deleteSnapshot(String snapshotName, RestHighLevelClient client) {
        try {
            LOGGER.debug("Deleting snapshot:{}", snapshotName);

            DeleteSnapshotRequest request = new DeleteSnapshotRequest()
                    .repository(REPOSITORY_NAME)
                    .snapshots(snapshotName);

            client.snapshot().delete(request, RequestOptions.DEFAULT);
        } catch(ElasticsearchStatusException | SnapshotMissingException | IOException e) {
            LOGGER.debug("Snapshot name:{} was not found for deletion", snapshotName);
        }
    }

    @Override
    public GetSnapshotsResponse getSnapshot(String snapshotName, RestHighLevelClient client) throws IOException {
        LOGGER.debug("Get repository:{}", REPOSITORY_NAME);

        GetSnapshotsRequest request = new GetSnapshotsRequest()
                .repository(REPOSITORY_NAME)
                .snapshots(new String[]{snapshotName});

        return client.snapshot().get(request, RequestOptions.DEFAULT);
    }

    @Override
    public void createRepository(RestHighLevelClient client) throws IOException {
        LOGGER.debug("Creating repository:{}", REPOSITORY_NAME);
        PutRepositoryRequest request = new PutRepositoryRequest()
                .name(REPOSITORY_NAME)
                .type(REPOSITORY_TYPE)
                .settings(Settings.builder()
                        .put("compress", true)
                        .put("location", REPOSITORY_LOCATION)
                        .build()
                );

        client.snapshot().createRepository(request, RequestOptions.DEFAULT);
    }

    @Override
    public GetRepositoriesResponse getRepository(RestHighLevelClient client) throws IOException {
        LOGGER.debug("Get repository:{}", REPOSITORY_NAME);

        GetRepositoriesRequest request = new GetRepositoriesRequest()
                .repositories(new String[]{REPOSITORY_NAME});

        return client.snapshot().getRepository(request, RequestOptions.DEFAULT);
    }

    @Override
    public void closeIndex(@NotNull String index, @NotNull RestHighLevelClient client) throws IOException {
        if(!isIndexClosed(index ,client)) {

            LOGGER.debug("Closing index:{}", index);

            CloseIndexRequest request = new CloseIndexRequest(index);

            client.indices().close(request, RequestOptions.DEFAULT);

        } else {
            LOGGER.debug("Attempting to close index:{} but it is already closed", index);
        }
    }

    @Override
    public void deleteIndex(@NotNull String index, @NotNull RestHighLevelClient client) throws IOException {
        LOGGER.debug("Deleting index:{}", index);

        DeleteIndexRequest request = new DeleteIndexRequest(index);

        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Override
    public void openIndex(@NotNull String index, @NotNull RestHighLevelClient client) throws IOException {
        LOGGER.debug("Opening index:{}", index);

        OpenIndexRequest request = new OpenIndexRequest()
                .indices(index);

        client.indices().open(request, RequestOptions.DEFAULT);
    }

    @Override
    public void forceMergeIndex(@NotNull String index, int maxSegments, @NotNull RestHighLevelClient client) throws IOException {
        if(!isIndexClosed(index, client)) {
            LOGGER.debug("Merging segments of index:{}", index);

            // [number of shards, number of segments]
            int[] counts = getSegmentCount(index, client);
            int shardCount = counts[0];
            int segmentCount = counts[1];

            if(segmentCount > (shardCount * maxSegments)) {
                ForceMergeRequest request = new ForceMergeRequest()
                        .indices(index)
                        .maxNumSegments(maxSegments);

                client.indices().forcemerge(request, RequestOptions.DEFAULT);
            }
        } else {
            LOGGER.debug("Attempting to merge index:{} but it is already closed", index);
        }
    }

    @Override
    public List<String> findAllIndices(@NotNull IndexOptions indexOptions, @NotNull RestHighLevelClient client) throws IOException {
        LOGGER.debug("Finding all indices using options:{}", indexOptions);
        String[] indices = getIndices(client, indexOptions.getPrefix(), indexOptions.isIncludeOpen(), indexOptions.isIncludeClosed());

        List<String> expiredIndices = new ArrayList<>();

        for(String index : indices) {
            String suffix = index.substring(indexOptions.getPrefix().length());
            LocalDate indexTime = getIndexDate(suffix);

            if(indexTime.isBefore(indexOptions.getCutoff())) {
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
    private String[] getIndices(RestHighLevelClient client, String prefix, boolean includeOpen, boolean includeClosed) throws IOException {
        GetSettingsRequest request = new GetSettingsRequest()
                .indices(prefix + WILDCARD)
                .indicesOptions(IndicesOptions.fromOptions(false, false, includeOpen, includeClosed));

        GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);

        return response.getIndexToSettings().keys().toArray(String.class);
    }

    /**
     * todo base this on size as well as last I knew 5 GB was the breaking point size for a segment
     * Get the total number of segments for the requested index.
     * @param index name to query
     * @param client {@link Client} for elasticsearch
     * @return Array [number of shards, number of segments]
     */
    public int[] getSegmentCount(String index, RestHighLevelClient client) throws IOException {
        String endpoint = index + "/_segments?filter_path=indices.*.shards.*.num_committed_segments&ignore_unavailable=true";
        Request request = new Request("GET", endpoint);

        Response response = client.getLowLevelClient().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode responseJson = objectMapper.readTree(EntityUtils.toString(response.getEntity()));
        Collection<Object> shards = Optional.ofNullable(responseJson.get("indices"))
                .map(i -> i.get(index))
                .map(i -> i.get("shards"))
                .map(s -> objectMapper.convertValue(s, new TypeReference<Map<String, Object>>(){}))
                .map(Map::values)
                .orElse(Collections.emptyList());

        int segments = 0;
        for (Object shard : shards) {
            segments += ((ArrayList<Map<String, Integer>>) shard)
                    .get(0)
                    .get("num_committed_segments");
        }

        return new int[]{shards.size(), segments};
    }

    /**
     * Check if the index is closed
     * todo I think this is no longer needed with the right options; may still be a useful operation to have in the bucket though
     * @param index to query
     * @param client {@link Client} for elasticsearch
     * @return boolean if index is closed
     * @throws IOException
     */
    public boolean isIndexClosed(String index, RestHighLevelClient client) throws IOException {
        String endpoint = "_cluster/state/metadata/" + index + "?filter_path=metadata.indices.*.state";
        Request request = new Request("GET", endpoint);

        Response response = client.getLowLevelClient().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode responseJson = objectMapper.readTree(EntityUtils.toString(response.getEntity()));

        return Optional.ofNullable(responseJson.get("metadata"))
                .map(m -> m.get("indices"))
                .map(i -> i.get(index))
                .map(i -> i.get("state"))
                .map(JsonNode::textValue)
                .map(s -> s.equalsIgnoreCase("close"))
                .orElse(true);
    }

    /**
     * Check if the snapshot exists for the index.
     * @param client {@link Client} that can connect to elasticsearch
     * @param snapshotName name of snapshot to lookup
     * @return boolean whether snapshot exists
     */
    protected boolean isSnapshotExists(RestHighLevelClient client, String snapshotName) {
        boolean exists = true;

        try {
            GetSnapshotsRequest request = new GetSnapshotsRequest()
                    .repository(REPOSITORY_NAME)
                    .snapshots(new String[]{snapshotName});

            client.snapshot().get(request, RequestOptions.DEFAULT);
        } catch(ElasticsearchStatusException | SnapshotMissingException | IOException e) {
            exists = false;
            LOGGER.debug("Snapshot name:{} is available to be created", snapshotName);
        }

        return exists;
    }
}
