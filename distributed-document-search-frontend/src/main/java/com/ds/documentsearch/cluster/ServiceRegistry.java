package com.ds.documentsearch.cluster;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

/**
 * This class provides methods for service registry. Servers can
 * register themselves as workers or coordinators under the corresponding
 * namespace and ask for all registered service addresses.
 */
@Component
public class ServiceRegistry implements Watcher {
    public static final String WORKERS_REGISTRY_NAMESPACE = "/workers_service_registry";
    public static final String COORDINATORS_REGISTRY_NAMESPACE = "/coordinators_service_registry";
    private static final Logger LOGGER = Logger.getLogger(ServiceRegistry.class.getName());

    private final ZooKeeper zooKeeper;
    private final String registryNamespace;
    private String currentZnode;
    private List<String> allServiceAddresses;
    private final Random random;

    @Autowired
    public ServiceRegistry(ZooKeeper zooKeeper, @Value("/coordinators_service_registry" )String registryNamespace) {
        this.zooKeeper = zooKeeper;
        this.registryNamespace = registryNamespace;
        createServiceRegistryNode();
        this.random = new Random();
    }

    public void registerToCluster(String serviceAddress) throws KeeperException, InterruptedException {
        if (currentZnode != null) {
            LOGGER.info(String.format("Already registered service address: %s to service registry.", serviceAddress));
            return;
        }
        currentZnode = zooKeeper.create(registryNamespace + "/n_", serviceAddress.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOGGER.info(String.format("Registered service address: %s to service registry with znode: %s ",
                serviceAddress, this.currentZnode));
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            LOGGER.severe("Failed to register as leader for service addresses updates: " + e.getMessage());
        }
    }

    public void unregisterFromCluster() {
        try {
            if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException | InterruptedException e) {
            LOGGER.severe("Failed to unregister from cluster: " + e.getMessage());
        }
    }

    public synchronized List<String> getAllServiceAddresses() {
        if (allServiceAddresses == null) {
            try {
                updateAddresses();
            } catch (KeeperException | InterruptedException e) {
                LOGGER.severe("Failed to get latest service addresses: " + e.getMessage());
                return new ArrayList<>();
            }
        }
        return allServiceAddresses;
    }

    public synchronized String getRandomServiceAddress() {
        if (allServiceAddresses == null) {
            try {
                updateAddresses();
            } catch (KeeperException | InterruptedException e) {
                LOGGER.severe("Failed to get latest service addresses: " + e.getMessage());
                return null;
            }
        }

        if (!allServiceAddresses.isEmpty()) {
            int randomIndex = random.nextInt(allServiceAddresses.size());
            return allServiceAddresses.get(randomIndex);
        } else {
            return null;
        }
    }

    private void createServiceRegistryNode() {
        try {
            if (zooKeeper.exists(registryNamespace, false) == null) {
                zooKeeper.create(registryNamespace, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException e) {
            LOGGER.severe("Failed to create service registry node: " + e.getMessage());
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workers = zooKeeper.getChildren(registryNamespace, this);

        List<String> addresses = new ArrayList<>(workers.size());

        for (String worker : workers) {
            String serviceFullPath = registryNamespace + "/" + worker;
            Stat stat = zooKeeper.exists(serviceFullPath, false);
            if (stat == null) {
                continue;
            }

            byte[] addressBytes = zooKeeper.getData(serviceFullPath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        allServiceAddresses = Collections.unmodifiableList(addresses);
        LOGGER.info(String.format("The cluster service addresses in namespace %s are: %s",
                registryNamespace, allServiceAddresses));
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            LOGGER.severe("Failed to update service addresses: " + e.getMessage());
        }
    }
}
