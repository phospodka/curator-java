package net.resonanceb.curator.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.resonanceb.curator.core.impl.OperationsImpl;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SnapshotClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class OperationsTest {

    @InjectMocks
    private Operations operations = new OperationsImpl() {
        // hackiest thing in the world until I can figure out how to mock the ClusterStateResponse
        protected boolean isIndexClosed(String index, Client client) throws IOException {
             return closed;
        }

        protected boolean isSnapshotExists(Client client, String snapshotName) {
            return exists;
        }
    };

    @Mock
    private RestHighLevelClient client;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private IndicesClient indicesClient;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private SnapshotClient snapshotClient;

    private boolean closed = false;

    private boolean exists = false;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(client.indices()).thenReturn(indicesClient);
        when(client.snapshot()).thenReturn(snapshotClient);
    }

    @Test
    public void testCreateSnapshot() throws Exception {
//        ArgumentCaptor<CreateSnapshotRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CreateSnapshotRequest.class);
//        ActionFuture<CreateSnapshotResponse> responseActionFuture = mock(ActionFuture.class);
//        when(clusterAdminClient.createSnapshot(any())).thenReturn(responseActionFuture);
//        operations.createSnapshot("index", client);
//
//        verify(clusterAdminClient).createSnapshot(requestArgumentCaptor.capture());
//        CreateSnapshotRequest request = requestArgumentCaptor.getValue();
//        assertEquals("elastic_repository", request.repository());
//        assertEquals("index", request.indices()[0]);
//        assertEquals("snapshot_index", request.snapshot());
//        assertTrue(request.waitForCompletion());
//        assertFalse(request.includeGlobalState());
    }

    @Test
    public void testDeleteSnapshot() throws Exception {
//        ArgumentCaptor<DeleteSnapshotRequest> requestArgumentCaptor = ArgumentCaptor.forClass(DeleteSnapshotRequest.class);
//        ActionFuture<DeleteSnapshotResponse> responseActionFuture = mock(ActionFuture.class);
//        when(clusterAdminClient.deleteSnapshot(any())).thenReturn(responseActionFuture);
//        operations.deleteSnapshot("snapshot_index", client);
//
//        verify(clusterAdminClient).deleteSnapshot(requestArgumentCaptor.capture());
//        DeleteSnapshotRequest request = requestArgumentCaptor.getValue();
//        assertEquals("elastic_repository", request.repository());
//        assertEquals("snapshot_index", request.snapshot());
    }

    @Test
    public void testGetSnapshot() throws Exception {
//        ArgumentCaptor<GetSnapshotsRequest> requestArgumentCaptor = ArgumentCaptor.forClass(GetSnapshotsRequest.class);
//        ActionFuture<GetSnapshotsResponse> responseActionFuture = mock(ActionFuture.class);
//        when(clusterAdminClient.getSnapshots(any())).thenReturn(responseActionFuture);
//        operations.getSnapshot("snapshot", client);
//
//        verify(clusterAdminClient).getSnapshots(requestArgumentCaptor.capture());
//        GetSnapshotsRequest request = requestArgumentCaptor.getValue();
//        assertEquals("elastic_repository", request.repository());
//        assertEquals("snapshot", request.snapshots()[0]);
    }

    @Test
    public void testCreateRepository() throws Exception {
//        ArgumentCaptor<PutRepositoryRequest> requestArgumentCaptor = ArgumentCaptor.forClass(PutRepositoryRequest.class);
//        ActionFuture<PutRepositoryResponse> responseActionFuture = mock(ActionFuture.class);
//        when(clusterAdminClient.putRepository(any())).thenReturn(responseActionFuture);
//        operations.createRepository(client);
//
//        verify(clusterAdminClient).putRepository(requestArgumentCaptor.capture());
//        PutRepositoryRequest request = requestArgumentCaptor.getValue();
//        assertEquals("elastic_repository", request.name());
//        assertEquals("fs", request.type());
//        assertEquals("/tmp/elasticsearch/archive", request.settings().get("location"));
//        assertTrue(request.settings().getAsBoolean("compress", false));
    }

    @Test
    public void testGetRepository() throws Exception {
//        ArgumentCaptor<GetRepositoriesRequest> requestArgumentCaptor = ArgumentCaptor.forClass(GetRepositoriesRequest.class);
//        ActionFuture<GetRepositoriesResponse> responseActionFuture = mock(ActionFuture.class);
//        when(clusterAdminClient.getRepositories(any())).thenReturn(responseActionFuture);
//        operations.getRepository(client);
//
//        verify(clusterAdminClient).getRepositories(requestArgumentCaptor.capture());
//        GetRepositoriesRequest request = requestArgumentCaptor.getValue();
//        assertEquals("elastic_repository", request.repositories()[0]);
    }

    @Test
    public void testCloseIndex() throws Exception {
//        ArgumentCaptor<CloseIndexRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CloseIndexRequest.class);
//        ActionFuture<CloseIndexResponse> responseActionFuture = mock(ActionFuture.class);
//        when(indicesAdminClient.close(any())).thenReturn(responseActionFuture);
//        operations.closeIndex("index", client);
//
//        verify(indicesAdminClient).close(requestArgumentCaptor.capture());
//        CloseIndexRequest request = requestArgumentCaptor.getValue();
//        assertEquals("index", request.indices()[0]);
    }

    @Test
    public void testDeleteIndex() throws Exception {
//        ArgumentCaptor<DeleteIndexRequest> requestArgumentCaptor = ArgumentCaptor.forClass(DeleteIndexRequest.class);
//        ActionFuture<DeleteIndexResponse> responseActionFuture = mock(ActionFuture.class);
//        when(indicesAdminClient.delete(any())).thenReturn(responseActionFuture);
//        operations.deleteIndex("index", client);
//
//        verify(indicesAdminClient).delete(requestArgumentCaptor.capture());
//        DeleteIndexRequest request = requestArgumentCaptor.getValue();
//        assertEquals("index", request.indices()[0]);
    }

    @Test
    public void testOpenIndex() throws Exception {
//        ArgumentCaptor<OpenIndexRequest> requestArgumentCaptor = ArgumentCaptor.forClass(OpenIndexRequest.class);
//        ActionFuture<OpenIndexResponse> responseActionFuture = mock(ActionFuture.class);
//        when(indicesAdminClient.open(any())).thenReturn(responseActionFuture);
//        operations.openIndex("index", client);
//
//        verify(indicesAdminClient).open(requestArgumentCaptor.capture());
//        OpenIndexRequest request = requestArgumentCaptor.getValue();
//        assertEquals("index", request.indices()[0]);
    }

    @Test
    public void testForceMergeIndex() throws Exception {
        // todo need to use elastic test framework to work with the responses since they are pretty impossible to mock
    }

    @Test
    public void testFindAllIndices() throws Exception {
        // todo need to use elastic test framework to work with the responses since they are pretty impossible to mock
    }
}
