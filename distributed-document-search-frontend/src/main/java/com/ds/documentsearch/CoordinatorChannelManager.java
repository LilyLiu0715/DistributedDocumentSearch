package com.ds.documentsearch;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.PreDestroy;

import com.ds.documentsearch.cluster.ServiceRegistry;

import org.springframework.stereotype.Component;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * This class maitains all opened coordinator channels.
 */
@Component
public class CoordinatorChannelManager {
    private static final Logger LOGGER = Logger.getLogger(CoordinatorChannelManager.class.getName());
    private static final int CHANNEL_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private ServiceRegistry coordinatorServiceRegistry;
    private ConcurrentHashMap<String, ManagedChannel> channels;

    public CoordinatorChannelManager(ServiceRegistry coordinatorServiceRegistry) {
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
        this.channels = new ConcurrentHashMap<>();
    }

    /**
     * Give a coordinator blocking stub from a random coordinator channel.
     * @return a coordinator stub.
     */
    public DocumentSearchCoordinatorServiceGrpc.DocumentSearchCoordinatorServiceBlockingStub getCoordinatorStub() {
        String coordinatorServiceAddress = coordinatorServiceRegistry.getRandomServiceAddress();
        if (coordinatorServiceAddress == null) {
            LOGGER.severe("No coordinator available.");
            return null;
        }

        if (!channels.containsKey(coordinatorServiceAddress)) {
            LOGGER.info("Initializing channel for a new service address" + coordinatorServiceAddress);
            ManagedChannel channel = ManagedChannelBuilder.forTarget(coordinatorServiceAddress).usePlaintext().build();
            channels.put(coordinatorServiceAddress, channel);
        }

        Channel channel = channels.get(coordinatorServiceAddress);
        return DocumentSearchCoordinatorServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Shut down all the coordinator channels.
     */
    @PreDestroy
    public void cleanUp() {
        for (String serviceAddress : channels.keySet()) {
            ManagedChannel channel = channels.get(serviceAddress);
            try {
                channel.shutdownNow().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.warning("Failed to shut down channel: " + serviceAddress);
            }
        }

    }
    
}
