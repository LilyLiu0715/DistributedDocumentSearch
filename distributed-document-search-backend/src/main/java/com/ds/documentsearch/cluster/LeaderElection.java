package com.ds.documentsearch.cluster;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * This class provides methods for leader election based on ZooKeeper.
 */
public class LeaderElection implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(LeaderElection.class.getName());
    public static final String ELECTION_NAMESPACE = "/election";

    private ZooKeeper zooKeeper;
    private OnElectionCallback onElectionCallback;
    private String currentZnodeName = null;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    /**
     * Volunteer to be the leader by creating a node to represent itself
     * under ZooKeeper election namespace.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        LOGGER.info(String.format("Registered to cluster with znode: %s.", this.currentZnodeName));
    }

    /**
     * (Re)-election upon server disconnection. Each server compares their own nodes
     * with the smallest one under election namespace. The server with the smallest
     * node is considered leader while others become workers.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorNode = "";
        while (predecessorStat == null) {
            List<String> servers = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(servers);
            LOGGER.info("All servers: " + servers);
            String smallestServer = servers.get(0);
            if (currentZnodeName.equals(smallestServer)) {
                LOGGER.info("On Election as leader.");
                onElectionCallback.onElectionAsLeader();
                return;
            } else {
                LOGGER.info("On Election as worker.");
                int predecessorIndex = Collections.binarySearch(servers, currentZnodeName) - 1;
                predecessorNode = servers.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorNode, this);
            }
        }
        LOGGER.info(String.format("Watching predecessor znode: %s.", predecessorNode));
        onElectionCallback.onElectionAsWorker();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (InterruptedException | KeeperException e) {
                    LOGGER.error("Failed to reelect leader: " + e.getMessage());
                }
        }
    }
}
