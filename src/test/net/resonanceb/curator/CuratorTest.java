package net.resonanceb.curator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.resonanceb.curator.config.ElasticClient;
import net.resonanceb.curator.core.IndexOptions;
import net.resonanceb.curator.core.Operations;
import net.resonanceb.curator.impl.CuratorImpl;

import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class CuratorTest {

    @InjectMocks
    private Curator curator = new CuratorImpl();

    @Mock
    private Operations operations;

    @Mock
    private ElasticClient elasticClient;

    @Mock
    private Client client;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(elasticClient.getClient()).thenReturn(client);
        when(operations.findAllIndices(any(IndexOptions.class), any(Client.class))).thenReturn(Arrays.asList("index1", "index2"));
    }

    @Test
    public void testClose() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.close();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(Client.class));
        verify(operations, times(2)).closeIndex(anyString(), any(Client.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }

    @Test
    public void testDelete() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.delete();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(Client.class));
        verify(operations, times(2)).deleteIndex(anyString(), any(Client.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertTrue(indexOptions.isIncludeClosed());
    }

    @Test
    public void testBackup() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.backup();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(Client.class));
        verify(operations, times(2)).createSnapshot(anyString(), any(Client.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }

    @Test
    public void testForceMerge() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.forceMerge();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(Client.class));
        verify(operations, times(2)).forceMergeIndex(anyString(), anyInt(), any(Client.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }
}
