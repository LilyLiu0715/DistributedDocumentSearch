package com.ds.documentsearch.service;

import com.ds.documentsearch.*;
import com.ds.documentsearch.utils.Tokenizer;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Worker service implementation. See proto definition for the interface.
 */
public class WorkerServiceImpl extends DocumentSearchWorkerServiceGrpc.DocumentSearchWorkerServiceImplBase {
    private static final Logger LOGGER = Logger.getLogger(WorkerServiceImpl.class.getName());
    private static final String BOOKS_DIRECTORY = "./resources/books/";

    private final Map<String, List<String>> documents;
    private final Tokenizer tokenizer;

    public WorkerServiceImpl(Tokenizer tokenizer) {
        this.tokenizer = tokenizer;
        this.documents = readAllDocuments();
    }

    @Override
    public void dispatchWork(WorkerRequest request, StreamObserver<WorkerResponse> responseObserver) {
        LOGGER.info("Received dispatch work request: " + request);

        WorkerResponse.Builder responseBuilder = WorkerResponse.newBuilder();
        for (String documentTitle : request.getDocumentTitlesList()) {
            if (!documents.containsKey(documentTitle)) {
                LOGGER.error("Missing document: " + documentTitle);
                continue;
            }
            List<String> document = documents.get(documentTitle);
            List<DocumentTermFrequencies.TermFrequency> termFrequencies =
                    calculateTermFrequencies(request.getTermsList(), document);

            DocumentTermFrequencies documentTermFrequencies = DocumentTermFrequencies.newBuilder()
                    .setDocumentTitle(documentTitle)
                    .addAllTermFrequencies(termFrequencies).build();
            responseBuilder.addDocumentTermFrequencies(documentTermFrequencies);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * Read all the documents from disk and parse them into tokens.
     * @return document tokens keyed by document titles.
     */
    private Map<String, List<String>> readAllDocuments() {
        Map<String, List<String>> documents = new HashMap<>();
        File documentsDirectory = new File(BOOKS_DIRECTORY);
        for (String documentTitle : documentsDirectory.list()) {
            String documentFullPath = BOOKS_DIRECTORY + "/" + documentTitle;
            try {
                FileReader fileReader = new FileReader(documentFullPath);
                try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                    List<String> words = bufferedReader.lines()
                            .map(tokenizer::tokenize)
                            .flatMap(List::stream)
                            .collect(Collectors.toList());
                    documents.put(documentTitle, words);
                } catch (IOException e) {
                    LOGGER.error(String.format("Failed to read document from %s. Error: %s",
                    documentFullPath, e.getMessage()));
                }
            } catch (FileNotFoundException e) {
                LOGGER.error(String.format("Failed to read document from %s. Error: %s",
                        documentFullPath, e.getMessage()));
            }
        }
        return documents;
    }

    /**
     * Calculate term frequencies in all the given documents.
     * @param terms all the terms.
     * @param document all the documents.
     * @return
     */
    private List<DocumentTermFrequencies.TermFrequency> calculateTermFrequencies(
            List<String> terms, List<String> document) {
        int[] termCounts = new int[terms.size()];
        for (String word : document) {
            for (int i = 0; i < terms.size(); i++) {
                if (word.equals(terms.get(i))) {
                    termCounts[i]++;
                }
            }
        }

        List<DocumentTermFrequencies.TermFrequency> termFrequencies = new ArrayList<>();
        for (int i = 0; i < termCounts.length; i++) {
            DocumentTermFrequencies.TermFrequency termFrequency =  DocumentTermFrequencies.TermFrequency.newBuilder()
                    .setTerm(terms.get(i))
                    .setTermFrequency((double) termCounts[i] / document.size()).build();
            termFrequencies.add(termFrequency);
        }

        return termFrequencies;
    }
}
