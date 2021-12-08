package com.ds.documentsearch;

import java.util.logging.Logger;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * The controller class that handles search requests.
 */
@Controller
public class SearchController {
    public static final Logger LOGGER = Logger.getLogger(SearchController.class.getName());
    public static final int DEFAULT_TOP_K = 5;

    private CoordinatorChannelManager channelManager;

    public SearchController(CoordinatorChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @GetMapping(value="/search")
    public String runQuery(@RequestParam(value = "query") String query, 
                           @RequestParam(value = "topK", required = false) Integer topK, 
                           Model model) {
        LOGGER.info(String.format("Received request for running query: %s", query));
        model.addAttribute("query", query);

        DocumentSearchCoordinatorServiceGrpc.DocumentSearchCoordinatorServiceBlockingStub coordinatorStub = channelManager.getCoordinatorStub();
        if (coordinatorStub == null) {
            return "search";
        }
        SearchRequest request = SearchRequest.newBuilder().setQuery(query).setTopK(topK == null ? DEFAULT_TOP_K : topK).build();
        SearchResponse response = coordinatorStub.searchDocument(request);

        model.addAttribute("results", response.getResultsList());

        return "search";
    }
}
