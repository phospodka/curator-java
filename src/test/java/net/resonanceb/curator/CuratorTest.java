package net.resonanceb.curator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.resonanceb.curator.config.ElasticClient;
import net.resonanceb.curator.core.IndexOptions;
import net.resonanceb.curator.core.Operations;
import net.resonanceb.curator.impl.CuratorImpl;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

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
    private RestHighLevelClient client;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(elasticClient.getClient()).thenReturn(client);
        when(operations.findAllIndices(any(IndexOptions.class), any(RestHighLevelClient.class))).thenReturn(Arrays.asList("index1", "index2"));
    }

    @Test
    public void testClose() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.close();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(RestHighLevelClient.class));
        verify(operations, times(2)).closeIndex(anyString(), any(RestHighLevelClient.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }

    @Test
    public void testDelete() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.delete();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(RestHighLevelClient.class));
        verify(operations, times(2)).deleteIndex(anyString(), any(RestHighLevelClient.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertTrue(indexOptions.isIncludeClosed());
    }

    @Test
    public void testBackup() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.backup();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(RestHighLevelClient.class));
        verify(operations, times(2)).createSnapshot(anyString(), any(RestHighLevelClient.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }

    @Test
    public void testForceMerge() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);

        curator.forceMerge();

        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(RestHighLevelClient.class));
        verify(operations, times(2)).forceMergeIndex(anyString(), anyInt(), any(RestHighLevelClient.class));

        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }
}
