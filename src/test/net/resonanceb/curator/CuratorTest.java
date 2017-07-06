package net.resonanceb.curator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(elasticClient.getClient()).thenReturn(client);
    }

    @Test
    public void testClose() throws Exception {
        ArgumentCaptor<IndexOptions> indexOptionsCaptor = ArgumentCaptor.forClass(IndexOptions.class);
        curator.close();
        verify(operations).findAllIndices(indexOptionsCaptor.capture(), any(Client.class));
        //verify(operations).closeIndex(anyString(), any(Client.class));
        IndexOptions indexOptions = indexOptionsCaptor.getValue();
        assertTrue(indexOptions.isIncludeOpen());
        assertFalse(indexOptions.isIncludeClosed());
    }

    @Test
    public void testDelete() throws Exception {

    }

    @Test
    public void testBackup() throws Exception {

    }

    @Test
    public void testForceMerge() throws Exception {

    }
}
