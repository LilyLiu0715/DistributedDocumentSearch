package com.ds.documentsearch.service;

import com.ds.documentsearch.*;
import com.ds.documentsearch.cluster.ServiceRegistry;
import com.ds.documentsearch.utils.Tokenizer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Coordinator service implementation. See proto definition for the interface.
 */
public class CoordinatorServiceImpl
        extends DocumentSearchCoordinatorServiceGrpc.DocumentSearchCoordinatorServiceImplBase {
    private static final Logger LOGGER = Logger.getLogger(CoordinatorServiceImpl.class.getName());
    private static final String BOOKS_DIRECTORY = "./resources/books/";

    private final Tokenizer tokenizer;
    private final List<String> documentTitles;
    private final ServiceRegistry serviceRegistry;
    private final Map<String, DocumentSearchWorkerServiceGrpc.DocumentSearchWorkerServiceFutureStub> workerStubs;

    public CoordinatorServiceImpl(
            Tokenizer tokenizer,
            ServiceRegistry serviceRegistry) {
        this.tokenizer = tokenizer;
        this.documentTitles = readDocumentTitles();
        this.serviceRegistry = serviceRegistry;
        this.workerStubs = new HashMap<>();
        for (String serviceAddress : serviceRegistry.getAllServiceAddresses()) {
            LOGGER.info("Initializing worker stub with address: " + serviceAddress);
            Channel channel = ManagedChannelBuilder.forTarget(serviceAddress).usePlaintext().build();
            workerStubs.put(serviceAddress, DocumentSearchWorkerServiceGrpc.newFutureStub(channel));
        }
    }

    @Override
    public void searchDocument(SearchRequest request, StreamObserver<SearchResponse> responseObserver) {
        LOGGER.info("Received dispatch work request: " + request);

        // Reject invalid requests.
        SearchResponse.Builder responseBuilder = SearchResponse.newBuilder();
        if (request.getQuery().isEmpty()) {
            LOGGER.error("Query cannot be empty.");
            ResponseStatus status = ResponseStatus.newBuilder()
                    .setResponseCode(ResponseStatus.ResponseCode.INVALID_REQUEST)
                    .setErrMsg("Query cannot be empty.").build();
            responseObserver.onNext(responseBuilder.setStatus(status).build());
            responseObserver.onCompleted();
            return;
        }

        // Terminate if there are no workers.
        List<String> serviceAddresses = serviceRegistry.getAllServiceAddresses();
        if (serviceAddresses.isEmpty()) {
            LOGGER.error("No worker is available.");
            ResponseStatus status = ResponseStatus.newBuilder()
                    .setResponseCode(ResponseStatus.ResponseCode.INTERNAL_FAILURE)
                    .setErrMsg("No worker is available.").build();
            responseObserver.onNext(responseBuilder.setStatus(status).build());
            responseObserver.onCompleted();
            return;
        }

        // Dispatch the work to each worker in parellel.
        List<String> terms = tokenizer.tokenize(request.getQuery());
        List<List<String>> workersDocuments = splitDocumentTitles(documentTitles, serviceAddresses.size());
        LOGGER.info("Split documents into: " + workersDocuments);
        Executor listeningExecutor = Executors.newSingleThreadExecutor();
        List<ListenableFuture<WorkerResponse>> workerTasks = new ArrayList<>();
        for (int i = 0; i < serviceAddresses.size(); i++) {
            String serviceAddress = serviceAddresses.get(i);
            if (!workerStubs.containsKey(serviceAddress)) {
                LOGGER.info("New worker available at " + serviceAddress);
                Channel channel = ManagedChannelBuilder.forTarget(serviceAddress).usePlaintext().build();
                workerStubs.put(serviceAddress, DocumentSearchWorkerServiceGrpc.newFutureStub(channel));
            }

            DocumentSearchWorkerServiceGrpc.DocumentSearchWorkerServiceFutureStub workerStub =
                    workerStubs.get(serviceAddress);
            List<String> workerDocumentTitles = workersDocuments.get(i);
            WorkerRequest workerRequest = WorkerRequest.newBuilder().addAllTerms(terms)
                    .addAllDocumentTitles(workerDocumentTitles).build();
            LOGGER.info(String.format("Sending worker request to worker at %s: %s", workerStub.getChannel().authority(),
                    workerRequest));
            workerTasks.add(workerStub.withDeadlineAfter(5000, TimeUnit.MILLISECONDS)
                    .dispatchWork(workerRequest));
        }

        // Collect worker responses and finalize the results.
        ListenableFuture<List<WorkerResponse>> allWorkerTasks = Futures.successfulAsList(workerTasks);
        Futures.addCallback(allWorkerTasks, new FutureCallback<List<WorkerResponse>>() {
            @Override
            public void onSuccess(@Nullable List<WorkerResponse> workerResponses) {
                List<SearchResponse.SearchResult> results = aggregateWorkerResponse(workerResponses, terms);
                int maxNumberOfResults = Math.min(request.getTopK(), documentTitles.size());
                responseBuilder.addAllResults(results.subList(0, maxNumberOfResults));
                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn(String.format("Failed to collect responses from workers. Error: %s", t.getMessage()));
                ResponseStatus status = ResponseStatus.newBuilder()
                        .setResponseCode(ResponseStatus.ResponseCode.INTERNAL_FAILURE)
                        .setErrMsg("Failed to collect responses from workers.").build();
                responseObserver.onNext(responseBuilder.setStatus(status).build());
                responseObserver.onCompleted();
            }
        }, listeningExecutor);
    }

    /**
     * Read titles of all the document for search.
     * @return
     */
    private List<String> readDocumentTitles() {
        File documentsDirectory = new File(BOOKS_DIRECTORY);
        String[] documentTitles = documentsDirectory.list();
        Arrays.sort(documentTitles);
        return Arrays.asList(documentTitles);
    }

    /**
     * Split the documents into batches based on the number of workers.
     * @param documentTitles the total document titiles.
     * @param numberOfWorkers the number of workers.
     * @return batches of document titiles that will be handled by the workers.
     */
    private List<List<String>> splitDocumentTitles(List<String> documentTitles, int numberOfWorkers) {
        int numberOfDocumentsPerWorker = documentTitles.size() / numberOfWorkers;
        int remainingDocuments = documentTitles.size() - numberOfDocumentsPerWorker * numberOfWorkers;

        List<List<String>> workersDocuments = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < numberOfWorkers; i++) {
            int firstDocumentIndex = index;
            int lastDocumentIndexExclusive = firstDocumentIndex + numberOfDocumentsPerWorker;
            if (remainingDocuments-- > 0) {
                lastDocumentIndexExclusive++;
            }

            workersDocuments.add(documentTitles.subList(firstDocumentIndex, lastDocumentIndexExclusive));
            index = lastDocumentIndexExclusive;
        }
        return workersDocuments;
    }

    /**
     * Aggregate all the worker reponses to generate the final search results.
     * @param workerResponses all the worker response.
     * @param terms all the terms for this search request.
     * @return the final search results sorted by relevance score.
     */
    private List<SearchResponse.SearchResult> aggregateWorkerResponse(
            List<WorkerResponse> workerResponses, List<String> terms) {
        int[] termOccurrences = new int[terms.size()];
        Arrays.fill(termOccurrences, 0);

        // First round: go through all the term frequncies to calculate term occurrences across all the documents.
        // This is needed for calculating inverse document frequencies in the secound round.
        for (WorkerResponse workerResponse : workerResponses) {
            for (DocumentTermFrequencies documentTermFrequencies : workerResponse.getDocumentTermFrequenciesList()) {
                for (int i = 0; i < documentTermFrequencies.getTermFrequenciesCount(); i++) {
                    if (documentTermFrequencies.getTermFrequencies(i).getTermFrequency() > 0.0) {
                        termOccurrences[i]++;
                    }
                }
            }
        }
        LOGGER.info("Term occurrences: " + Arrays.toString(termOccurrences));

        // Second round: calculate relevance scores for each document by summing up all the
        // TF-IDF scores.
        List<SearchResponse.SearchResult> results = new ArrayList<>();
        for (WorkerResponse workerResponse : workerResponses) {
            for (DocumentTermFrequencies documentTermFrequencies : workerResponse.getDocumentTermFrequenciesList()) {
                String documentTitle = documentTermFrequencies.getDocumentTitle();
                double score = 0.0;
                for (int i = 0; i < documentTermFrequencies.getTermFrequenciesCount(); i++) {
                    double inverseDocumentFrequency = termOccurrences[i] == 0 ?
                            0 : Math.log10(documentTitles.size() * 1.0 / termOccurrences[i]);
                    double termFrequency = documentTermFrequencies.getTermFrequencies(i).getTermFrequency();
                    score += termFrequency * inverseDocumentFrequency;
                }
                SearchResponse.SearchResult result = SearchResponse.SearchResult.newBuilder()
                        .setDocumentTitle(documentTitle)
                        .setScore(score).build();
                results.add(result);
            }
        }

        // Sort the results.
        Collections.sort(results, (r1, r2) -> r1.getScore() < r2.getScore() ? 1 : -1);
        return results;
    }
}
