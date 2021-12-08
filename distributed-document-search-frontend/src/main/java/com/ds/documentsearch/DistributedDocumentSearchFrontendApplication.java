package com.ds.documentsearch;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DistributedDocumentSearchFrontendApplication implements Watcher {
    private static final Logger LOGGER = Logger.getLogger(DistributedDocumentSearchFrontendApplication.class.getName());

    private static final int SESSION_TIMEOUT = 3000;

	private ZooKeeper zooKeeper;

	@Bean
	public ZooKeeper connectToZooKeeper(@Value("${zookeeper.address}") String zooKeeperAddress) throws IOException {
        this.zooKeeper = new ZooKeeper(zooKeeperAddress, SESSION_TIMEOUT, this);
		return zooKeeper;
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

	public static void main(String[] args) {
		SpringApplication.run(DistributedDocumentSearchFrontendApplication.class, args);
	}
}
