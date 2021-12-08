package com.ds.documentsearch;

import com.ds.documentsearch.cluster.LeaderElection;
import com.ds.documentsearch.cluster.ServiceRegistry;
import com.ds.documentsearch.service.CoordinatorServiceImpl;
import com.ds.documentsearch.service.WorkerServiceImpl;
import com.ds.documentsearch.utils.Tokenizer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The backend server class. Each backend server hosts both coordinator and worker
 * service. But only the one that won leader election will be the coordinator and
 * receive requests from frontend servers.
 */
public class DocumentSearchBackendServer implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(DocumentSearchBackendServer.class.getName());
    private static final String STOP_WORDS_FILENAME = "./resources/stopwords.txt";

    // The address of the remotely deployed ZooKeeper instance.
    private static final String ZOOKEEPER_ADDRESS = "35.184.6.233:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8080;

    private final int port;
    private final Tokenizer tokenizer;
    private ZooKeeper zooKeeper;
    private ServiceRegistry workerServiceRegistry;
    private Server server;

    public DocumentSearchBackendServer(int port) {
        this.port = port;
        this.tokenizer = new Tokenizer(readStopWords());
    }

    /**
     * Connect to the ZooKeeper server.
     * @return the ZooKeeper instance connected to.
     * @throws IOException
     */
    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void setWorkerServiceRegistry(ServiceRegistry workerServiceRegistry) {
        this.workerServiceRegistry = workerServiceRegistry;
    }

    /**
     * Start both coordinator and worker services. Then wait for
     * ZooKeeper events.
     * @throws IOException
     * @throws InterruptedException
     */
    public void run() throws IOException, InterruptedException {
        server = ServerBuilder.forPort(port)
                .addService(new CoordinatorServiceImpl(tokenizer, workerServiceRegistry))
                .addService(new WorkerServiceImpl(tokenizer))
                .build().start();
        server.awaitTermination();
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    /**
     * Turns down gRPC services and ZooKeeper connection.
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        zooKeeper.close();
    }

    /**
     * Read the static stop word files.
     * @return a set of stop words.
     */
    private Set<String> readStopWords() {
        try {
            FileReader fileReader = new FileReader(STOP_WORDS_FILENAME);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            return bufferedReader.lines().collect(Collectors.toSet());
        } catch (FileNotFoundException e) {
            LOGGER.error(String.format("Failed to read stop words from %s. Error: %s",
                    STOP_WORDS_FILENAME, e.getMessage()));
            return new HashSet<>();
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

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        DocumentSearchBackendServer server = new DocumentSearchBackendServer(port);
        ZooKeeper zooKeeper = server.connectToZookeeper();

        ServiceRegistry workerServiceRegistry =
                new ServiceRegistry(zooKeeper, ServiceRegistry.WORKERS_REGISTRY_NAMESPACE);
        ServiceRegistry coordinatorServiceRegistry =
                new ServiceRegistry(zooKeeper, ServiceRegistry.COORDINATORS_REGISTRY_NAMESPACE);
        server.setWorkerServiceRegistry(workerServiceRegistry);
        LeaderElection leaderElection = new LeaderElection(zooKeeper,
                new OnElectionAction(workerServiceRegistry, coordinatorServiceRegistry, port));
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        server.run();
        server.close();
        LOGGER.info("Disconnected from Zookeeper, exiting application");
    }
}



