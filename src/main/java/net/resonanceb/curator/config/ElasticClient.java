package net.resonanceb.curator.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ElasticClient {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public RestHighLevelClient getClient() {
        return new RestHighLevelClient(RestClient.builder(buildHosts()));
    }

    // todo inclue scheme
    private HttpHost[] buildHosts() {
        return Arrays.stream(net.resonanceb.curator.config.Settings.hostsLists.split(","))
                .map(hostEntry -> hostEntry.split(":"))
                .map(entry -> new HttpHost(String.valueOf(entry[0]), Integer.parseInt(entry[1])))
                .collect(Collectors.toList())
                .toArray(HttpHost[]::new);

    }
}
