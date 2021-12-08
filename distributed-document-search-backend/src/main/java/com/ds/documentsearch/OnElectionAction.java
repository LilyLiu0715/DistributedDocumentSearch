package com.ds.documentsearch;

import com.ds.documentsearch.cluster.OnElectionCallback;
import com.ds.documentsearch.cluster.ServiceRegistry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A class that implements OnElectionCallback.
 * When elected as leader, the server will unregister from the worker registry (because
 * it might previsouly be a worker) and register itself to coordinator registry.
 * When elected as worker, the server will register itself to worker registry.
 */
public class OnElectionAction implements OnElectionCallback {
    private static final Logger LOGGER = Logger.getLogger(OnElectionAction.class.getName());

    private final ServiceRegistry workerServiceRegistry;
    private final ServiceRegistry coordinatorServiceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry workerServiceRegistry,
                            ServiceRegistry coordinatorServiceRegistry,
                            int port) {
        this.workerServiceRegistry = workerServiceRegistry;
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
        this.port = port;
    }

    @Override
    public void onElectionAsLeader() {
        workerServiceRegistry.unregisterFromCluster();
        workerServiceRegistry.registerForUpdates();

        try {
            String currentServerAddress =
                    String.format("%s:%d", InetAddress.getLocalHost().getHostAddress(), port);
            coordinatorServiceRegistry.registerToCluster(currentServerAddress);
        } catch (InterruptedException | UnknownHostException | KeeperException e) {
            LOGGER.error("Failed to register coordinator service: " + e.getMessage());
        }
    }

    @Override
    public void onElectionAsWorker() {
        try {
            String currentServerAddress =
                String.format("%s:%d", InetAddress.getLocalHost().getHostAddress(), port);
            workerServiceRegistry.registerToCluster(currentServerAddress);
        } catch (InterruptedException | UnknownHostException | KeeperException e) {
            LOGGER.error("Failed to register worker service: " + e.getMessage());
        }
    }
}
