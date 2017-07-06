package net.resonanceb.curator.config;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class ElasticClient {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public Client getClient() {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", net.resonanceb.curator.config.Settings.cluster)
                .put("client.transport.sniff", true)
                .build();

        Client client = TransportClient.builder().settings(settings).build();

        Arrays.stream(net.resonanceb.curator.config.Settings.hostsLists.split(","))
                .map(hostEntry -> hostEntry.split(":"))
                .forEach((entry) -> {
                    try {
                        ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(entry[0]), Integer.valueOf(entry[1])));
                    } catch (UnknownHostException e) {
                        LOGGER.error("Unable to connect to host: " + Arrays.toString(entry), e);
                    }
                });

        return client;
    }
}
