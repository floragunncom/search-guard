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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.floragunn.signals.watch.result.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.jetbrains.annotations.NotNull;

public class LoadOperatorSummaryHandler extends Handler<LoadOperatorSummaryRequest, StandardResponse> {

    private static final Logger log = LogManager.getLogger(LoadOperatorSummaryHandler.class);
    private static final int DEFAULT_MAX_WATCH_RESULTS = 1500;
    public static final String REASON_FAILED_WATCH = "failed_watch";
    public static final String REASON_NEVER_EXECUTED_WATCH_WITH_SEVERITY = "never_executed_watch_with_severity";
    public static final String REASON_MATCH_FILTER = "match_filter";

    private final WatchStateRepository watchStateRepository;
    private final WatchRepository watchRepository;

    private final Signals signals;


    @Inject
    public LoadOperatorSummaryHandler(HandlerDependencies handlerDependencies, NodeClient nodeClient,
        Signals signals) {
        super(LoadOperatorSummaryAction.INSTANCE, handlerDependencies);
        this.signals = signals;
        this.watchStateRepository = new WatchStateRepository(getStateIndexName(), PrivilegedConfigClient.adapt(nodeClient));
        this.watchRepository = new WatchRepository(getWatchIndexName(), PrivilegedConfigClient.adapt(nodeClient));
    }

    @Override
    protected CompletableFuture<StandardResponse> doExecute(LoadOperatorSummaryRequest request) {
        return supplyAsync(() -> {
            try {
                String tenant = request.getTenant();
                List<WatchActionNames> watchesWithSeveritiesPreFilteredById = watchRepository.searchWatchIdsWithSeverityAndIdPrefix(tenant,
                        request.getWatchFilter().getWatchId(), DEFAULT_MAX_WATCH_RESULTS);
                List<String> initialWatchIdsList = watchesWithSeveritiesPreFilteredById.stream() //
                        .map(WatchActionNames::watchIdWithTenantPrefix) //
                        .collect(Collectors.toList());

                LoadOperatorSummaryData failedWatchesData = loadFailedWatches(request, initialWatchIdsList);
                LoadOperatorSummaryData notExecutedWatchesWithSeverity = loadNeverExecutedWatchesWithSeverity(tenant, //
                        request.getSizeOrDefault() - failedWatchesData.getSize(), //
                        watchesWithSeveritiesPreFilteredById);

                LoadOperatorSummaryData loadOperatorSummaryData = loadWatchesByFilter(request.getSortingOrDefault(), //
                        request.getSizeOrDefault() - failedWatchesData.getSize() - notExecutedWatchesWithSeverity.getSize(), //
                        request.getWatchFilter(), initialWatchIdsList) //
                        .filterActions(watchesWithSeveritiesPreFilteredById);

                LoadOperatorSummaryData result = failedWatchesData.with(notExecutedWatchesWithSeverity) //
                        .with(loadOperatorSummaryData) //
                        .withActive(watchesWithSeveritiesPreFilteredById);
                return new StandardResponse(200).data(result);
            } catch (Exception ex) {
                log.error("Cannot load signal watch state summary", ex);
                return new StandardResponse(400).error(ex.getMessage());
            }
        });
    }

    private @NotNull LoadOperatorSummaryData loadWatchesByFilter(String sortingString, int size, WatchFilter watchFilter,
            List<String> watchIds) {
        if ((size <= 0) || watchIds.isEmpty()) {
            return new LoadOperatorSummaryData(List.of());
        }
        List<SortByField> sorting = SortParser.parseSortingExpression(sortingString);
        SearchResponse search = watchStateRepository.search(watchFilter, sorting, size, watchIds);
        try {
            return convertSearchResultToResponse(search, REASON_MATCH_FILTER);
        } finally {
            search.decRef();
        }
    }

    private LoadOperatorSummaryData loadNeverExecutedWatchesWithSeverity(String tenant, int size, List<WatchActionNames> watchesWithSeveritiesPreFilteredById) {
        List<String> watchIds = watchesWithSeveritiesPreFilteredById.stream() //
                .map(WatchActionNames::watchIdWithTenantPrefix) //
                .toList();
        if ((size <= 0) || watchIds.isEmpty()) {
            return new LoadOperatorSummaryData(List.of());
        }
        SearchResponse response = watchStateRepository.findNeverExecutedWatchesWithSeverity(tenant, watchIds, size);
        return convertSearchResultToResponse(response, REASON_NEVER_EXECUTED_WATCH_WITH_SEVERITY).filterActions(watchesWithSeveritiesPreFilteredById);
    }

    private @NotNull LoadOperatorSummaryData loadFailedWatches(LoadOperatorSummaryRequest request, Collection<String> watchIdsFilter) {
        List<Status.Code> statusCodes = List.of(Status.Code.ACTION_FAILED, Status.Code.EXECUTION_FAILED);
        LoadOperatorSummaryRequest failedWatchesRequest = request.withWatchStatusCodes(statusCodes);
        List<SortByField> sorting = SortParser.parseSortingExpression(request.getSortingOrDefault());
        try {
            SearchResponse failedWatchesResponse = watchStateRepository.search(failedWatchesRequest.getWatchFilter(), sorting, request.getSizeOrDefault(), watchIdsFilter);
            return convertSearchResultToResponse(failedWatchesResponse, REASON_FAILED_WATCH);
        } finally {
            failedWatchesRequest.decRef();
        }
    }

    private LoadOperatorSummaryData convertSearchResultToResponse(SearchResponse searchResponse, String reason) {
        log.debug("Watch state search result '{}'", searchResponse);
        List<WatchSummary> watches = Arrays.stream(searchResponse.getHits() //
            .getHits())//
            .map(hit -> this.toWatchSummary(hit, reason))//
            .collect(Collectors.toList());
        return new LoadOperatorSummaryData(watches);
    }

    private WatchSummary toWatchSummary(SearchHit documentFields, String reason) {
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
                mapActionSummary, reason, null);
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
            String ackBy = null;
            Instant ackOn = null;
            DocNode ackedNode = node.getAsNode("acked");
            if(Objects.nonNull(ackedNode)) {
                ackBy = ackedNode.getAsString("by");
                ackOn = getInstantValue(ackedNode, "on");
            }
            return new ActionSummary(getInstantValue(node, "last_triggered"),
                getInstantValue(node, "last_check"),
                node.getBoolean("last_check_result"),
                getInstantValue(node, "last_execution"),
                node.getAsString("last_error"),
                statusCode,
                statusDetails,
                ackBy,
                ackOn);
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

    private String getWatchIndexName() {
        return signals.getSignalsSettings().getStaticSettings().getIndexNames().getWatches();
    }
}
