package com.ds.documentsearch.cluster;

/**
 * An interface for the callback to be run after
 * election is finished.
 */
public interface OnElectionCallback {
    void onElectionAsLeader();

    void onElectionAsWorker();
}
