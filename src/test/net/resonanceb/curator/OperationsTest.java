package net.resonanceb.curator;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.resonanceb.curator.impl.OperationsImpl;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OperationsTest {

    @InjectMocks
    private Operations operations = new OperationsImpl();

    @Mock
    private Client client;

    @Mock
    private AdminClient adminClient;

    @Mock
    private ClusterAdminClient clusterAdminClient;

    @Captor
    protected ArgumentCaptor<GetSnapshotsRequest> getSnapshotsRequestArgumentCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    @Test
    public void testGetSnapshot() throws Exception {
        ActionFuture<GetSnapshotsResponse> getSnapshotsResponseActionFuture = mock(ActionFuture.class);
        when(clusterAdminClient.getSnapshots(any())).thenReturn(getSnapshotsResponseActionFuture);
        operations.getSnapshot("snapshot", client);

        verify(clusterAdminClient).getSnapshots(getSnapshotsRequestArgumentCaptor.capture());
        GetSnapshotsRequest getSnapshotsRequest = getSnapshotsRequestArgumentCaptor.getValue();
        assertEquals("elastic_repository", getSnapshotsRequest.repository());
        assertEquals("snapshot", getSnapshotsRequest.snapshots()[0]);
    }
}
