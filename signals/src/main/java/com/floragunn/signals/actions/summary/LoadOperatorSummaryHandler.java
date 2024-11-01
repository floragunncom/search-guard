package com.floragunn.signals.actions.summary;

import static com.floragunn.signals.actions.summary.SafeDocNodeReader.findSingleNodeByJsonPath;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getDoubleValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getInstantValue;
import static com.floragunn.signals.actions.summary.SafeDocNodeReader.getLongValue;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.searchsupport.action.Action.Handler;
import com.floragunn.searchsupport.action.Action.HandlerDependencies;
import com.floragunn.searchsupport.action.StandardResponse;
import com.floragunn.signals.Signals;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryData.ActionSummary;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryData.WatchSeverityDetails;
import com.floragunn.signals.actions.summary.LoadOperatorSummaryData.WatchSummary;
import com.floragunn.signals.actions.summary.SortParser.SortByField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

public class LoadOperatorSummaryHandler extends Handler<LoadOperatorSummaryRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(LoadOperatorSummaryHandler.class);

    private final WatchStateRepository watchStateRepository;

    private final Signals signals;


    @Inject
    public LoadOperatorSummaryHandler(HandlerDependencies handlerDependencies, NodeClient nodeClient,
        Signals signals) {
        super(LoadOperatorSummaryAction.INSTANCE, handlerDependencies);
        this.signals = signals;
        this.watchStateRepository = new WatchStateRepository(getStateIndexName(), PrivilegedConfigClient.adapt(nodeClient));
    }

    @Override
    protected CompletableFuture<StandardResponse> doExecute(LoadOperatorSummaryRequest request) {
       return supplyAsync(() -> {
            try {
                List<SortByField> sorting = SortParser.parseSortingExpression(request.getSorting());
                SearchResponse search = watchStateRepository.search(request.getWatchFilter(), sorting);
                LoadOperatorSummaryData loadOperatorSummaryData = convertSearchResultToResponse(search);
                return new StandardResponse(200).data(loadOperatorSummaryData);
            } catch (Exception ex) {
                log.error("Cannot load signal watch state summary", ex);
                return new StandardResponse(400).error(ex.getMessage());
            }
       });
    }

    private LoadOperatorSummaryData convertSearchResultToResponse(SearchResponse searchResponse) {
        log.debug("Watch state search result '{}'", searchResponse);
        List<WatchSummary> watches = Arrays.stream(searchResponse.getHits().getHits())//
            .map(this::toWatchSummary)//
            .collect(Collectors.toList());
        return new LoadOperatorSummaryData(watches);
    }

    private WatchSummary toWatchSummary(SearchHit documentFields) {
        try {
            DocNode docNode = DocNode.parse(Format.JSON).from(documentFields.getSourceAsString());
            DocNode docNodeLastStatus = findSingleNodeByJsonPath(docNode, "last_status").orElse(DocNode.EMPTY);
            WatchSeverityDetails severityDetails = findSingleNodeByJsonPath(docNode, "last_execution.severity")//
                .map(this::nodeToWatchSeverity)//
                .orElse(null);
            DocNode actions = docNode.getAsNode("actions");
            Map<String, ActionSummary> mapActionSummary = new HashMap<>();
            for(String key : actions.keySet()) {
                mapActionSummary.put(key, nodeToActionDetails(actions.getAsNode(key)));
            }
            return new WatchSummary(documentFields.getId(), docNodeLastStatus.getAsString("code"),
                docNodeLastStatus.getAsString("severity"), docNodeLastStatus.getAsString("detail"), severityDetails,
                mapActionSummary);
        } catch (DocumentParseException e) {
            throw new ElasticsearchStatusException("Cannot parse watch state search response", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private ActionSummary nodeToActionDetails(DocNode node)  {
        try {
            DocNode lastStatus = node.getAsNode("last_status");
            String statusCode = null;
            String statusDetails = null;
            if(Objects.nonNull(lastStatus)) {
                statusCode = lastStatus.getAsString("code");
                statusDetails = lastStatus.getAsString("detail");
            }
            return new ActionSummary(getInstantValue(node, "last_triggered"),
                getInstantValue(node, "last_check"),
                node.getBoolean("last_check_result"),
                getInstantValue(node, "last_execution"),
                node.getAsString("last_error"),
                statusCode,
                statusDetails);
        } catch (ConfigValidationException e) {
            throw new ElasticsearchStatusException("Cannot parse watch state action details", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private WatchSeverityDetails nodeToWatchSeverity(DocNode node) {
        return new WatchSeverityDetails(
            node.getAsString("level"),
            getLongValue(node, "level_numeric"),
            getDoubleValue(node, "value"),
            getDoubleValue(node, "threshold"));
    }

    private String getStateIndexName() {
        return signals.getSignalsSettings().getStaticSettings().getIndexNames().getWatchesState();
    }
}
