package com.ds.documentsearch.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A simple tokenizer class that parses input text into terms.
 * Stop words will be skipped.
 */
public class Tokenizer {
    private Set<String> stopWords;

    public Tokenizer(Set<String> stopWords) {
        this.stopWords = stopWords;
    }

    public List<String> tokenize(String input) {
        List<String> words = new ArrayList<>();
        for (String token : input.toLowerCase().split("[^\\w']+")) {
            if (!token.isEmpty() && !stopWords.contains(token)) {
                words.add(token);
            }
        }
        return words;
    }
}
