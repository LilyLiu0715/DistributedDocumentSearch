package com.ds.documentsearch;

import com.ds.documentsearch.cluster.ServiceRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * A simple client that is used for testing the backend cluster.
 */
public class DocumentSearchClient implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(DocumentSearchClient.class.getName());

    // The address of the remotely deployed ZooKeeper instance.
    private static final String ZOOKEEPER_ADDRESS = "35.184.6.233:2181";
    private static final int SESSION_TIMEOUT = 3000;

    private ZooKeeper zooKeeper;
    private ServiceRegistry coordinatorServiceRegistry;

    public DocumentSearchClient() {}

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void setCoordinatorServiceRegistry(ServiceRegistry coordinatorServiceRegistry) {
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
    }

    /**
     * Accept user input as query from commandline and send it
     * to a coordinator.
     * @throws IOException
     */
    public void run() throws IOException {
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            LOGGER.info("Enter query:");
            String userInput = stdIn.readLine();
            if (userInput != null) {
                String coordinatorServiceAddress = coordinatorServiceRegistry.getRandomServiceAddress();
                if (coordinatorServiceAddress == null) {
                    LOGGER.error("No coordinator available");
                    continue;
                }

                ManagedChannel channel = ManagedChannelBuilder.forTarget(coordinatorServiceAddress)
                        .usePlaintext().build();
                DocumentSearchCoordinatorServiceGrpc.DocumentSearchCoordinatorServiceBlockingStub coordinatorStub =
                        DocumentSearchCoordinatorServiceGrpc.newBlockingStub(channel);
                SearchRequest request = SearchRequest.newBuilder().setQuery(userInput).setTopK(5).build();
                SearchResponse response = coordinatorStub.searchDocument(request);
                LOGGER.info("Search response: " + response);

                try {
                    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOGGER.info("Failed to shutdown the channel: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    LOGGER.info("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        LOGGER.info("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }

    public static void main(String[] args) throws IOException {
        DocumentSearchClient server = new DocumentSearchClient();
        ZooKeeper zooKeeper = server.connectToZookeeper();

        ServiceRegistry coordinatorServiceRegistry =
                new ServiceRegistry(zooKeeper, ServiceRegistry.COORDINATORS_REGISTRY_NAMESPACE);
        server.setCoordinatorServiceRegistry(coordinatorServiceRegistry);

        server.run();
    }
}
