package net.resonanceb.curator;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * Elasticsearch individual admin operations to apply to an index.  Meant as the backbone so that someone
 * can work the management of indices into their own processes if needed.
 */
public interface Operations {

    /**
     * Create a snapshot of the index to the system defined repository.
     * @param index to snapshot
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void createSnapshot(@NotNull String index, @NotNull Client client) throws IOException;

    /**
     * Delete a snapshot of the index to the system defined repository.
     * @param snapshot name to delete
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void deleteSnapshot(@NotNull String snapshot, @NotNull Client client) throws IOException;

    /**
     * Get the snapsnot from the system defined repository.
     * @param snapshotName name of snapshot to get
     * @param client {@link Client} for elasticsearch
     * @return {@link GetSnapshotsResponse}
     * @throws IOException
     */
    GetSnapshotsResponse getSnapshot(String snapshotName, Client client) throws IOException;

    /**
     * Create a repository to the system defined location.
     * todo make not system defined
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void createRepository(Client client) throws IOException;

    /**
     * Get the system defined repository.
     * todo make with the not system defined
     * @param client {@link Client} for elasticsearch
     * @return {@link GetRepositoriesResponse}
     * @throws IOException
     */
    GetRepositoriesResponse getRepository(Client client) throws IOException;

    /**
     * Close the index.  This prevents it from being searched but is still stored in the cluster state.
     * @param index to close
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void closeIndex(@NotNull String index, @NotNull Client client) throws IOException;

    /**
     * Delete the index.  This permanently removes it from the file system.
     * @param index to delete
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void deleteIndex(@NotNull String index, @NotNull Client client) throws IOException;

    /**
     * Open the index.  This reopens a previously closed index to make it available for search.
     * @param index to open
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void openIndex(@NotNull String index, @NotNull Client client) throws IOException;

    /**
     * Force merge the segments of an index.  This will reduce the number of segments segments to the max provided.
     * @param index to merge
     * @param maxSegments max number of segments allowed per shard
     * @param client {@link Client} for elasticsearch
     * @throws IOException
     */
    void forceMergeIndex(@NotNull String index, int maxSegments, @NotNull Client client) throws IOException;


    /**
     * Find the list of all indices that are before the cutoff time.
     * @param options {@link Options} options to use with the find all operation
     * @param client {@link Client} for elasticsearch
     * @return {@link List} of indices that match the options
     * @throws IOException
     */
    List<String> findAllIndices(@NotNull Options options, @NotNull Client client) throws IOException;
}