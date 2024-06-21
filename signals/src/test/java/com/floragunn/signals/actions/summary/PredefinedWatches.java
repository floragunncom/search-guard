package com.floragunn.signals.actions.summary;

import static com.floragunn.signals.watch.severity.SeverityLevel.CRITICAL;
import static com.floragunn.signals.watch.severity.SeverityLevel.ERROR;
import static com.floragunn.signals.watch.severity.SeverityLevel.INFO;
import static com.floragunn.signals.watch.severity.SeverityLevel.NONE;
import static com.floragunn.signals.watch.severity.SeverityLevel.WARNING;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchguard.test.GenericRestClient;
import com.floragunn.searchguard.test.GenericRestClient.HttpResponse;
import com.floragunn.searchguard.test.TestSgConfig.User;
import com.floragunn.searchguard.test.helper.cluster.LocalCluster;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.WatchBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

class PredefinedWatches {

    private static final Logger log = LogManager.getLogger(PredefinedWatches.class);
    public static final String ACTION_CREATE_ALARM_ONE = "create_alarmOne";
    public static final String ACTION_CREATE_ALARM_TWO = "create_alarmTwo";
    private final LocalCluster.Embedded localCluster;
    private final User watchUser;
    private final String tenant;
    private List<WatchPointer> watchesToDelete;

    public PredefinedWatches(LocalCluster.Embedded localCluster, User watchUser, String tenant) {
        this.localCluster = requireNonNull(localCluster);
        this.watchUser = requireNonNull(watchUser, "Watch user is required");
        this.tenant = requireNonNull(tenant, "Tenant is required");
        this.watchesToDelete = new ArrayList<>();
    }

    public WatchPointer defineTemperatureSeverityWatch(String watchId, String inputTemperatureIndex, String outputAlarmIndex,
        double temperatureLimit, String actionName) {
        final String aggregationNameMaxTemperature = "max_temperature";
        final String maxTemperatureSearch = "max_temperature_search";
        final String maxTemperatureLimitName = "max_temperature";
        final String staticLimitsName = "limits";
        String tooHighTemperatureCondition = new StringBuilder("data.")//
            .append(maxTemperatureSearch)
            .append(".aggregations.")//
            .append(aggregationNameMaxTemperature)//
            .append(".value > data.")//
            .append(staticLimitsName)//
            .append(".")//
            .append(maxTemperatureLimitName)//
            .toString();
        try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
            DocNode aggregation = DocNode.of(aggregationNameMaxTemperature, DocNode.of("max", DocNode.of("field", "temperature")));

            Watch watch = new WatchBuilder(watchId)
                .triggerNow()
                .search(inputTemperatureIndex).size(0).aggregation(aggregation).as(maxTemperatureSearch)//
                .put(String.format("{\"%s\": %.2f}", maxTemperatureLimitName, temperatureLimit)).as(staticLimitsName)//
                .checkCondition(tooHighTemperatureCondition)//
                .consider("data." + maxTemperatureSearch + ".aggregations." + aggregationNameMaxTemperature + ".value")//
                    .greaterOrEqual(0).as(NONE)//
                    .greaterOrEqual(3).as(INFO)//
                    .greaterOrEqual(7).as(WARNING)//
                    .greaterOrEqual(10).as(ERROR)//
                    .greaterOrEqual(15).as(CRITICAL)//
                .then().index(outputAlarmIndex).name(actionName).build();
            String watchPath = createWatchPath(watchId);
            log.info("Predefined watch will be created using path '{}' and body '{}'", watchPath, watch.toJson());
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            WatchPointer watchPointer = new WatchPointer(watchPath);
            watchesToDelete.add(watchPointer);
            return watchPointer;
        } catch (Exception e) {
            throw new RuntimeException("Cannot define test watch", e);
        }
    }

    public WatchPointer defineSimpleTemperatureWatch(String watchId, String inputTemperatureIndex, String outputAlarmIndex,
        double temperatureLimit) {
        final String aggregationNameMaxTemperature = "max_temperature";
        final String maxTemperatureSearch = "max_temperature_search";
        final String maxTemperatureLimitName = "max_temperature";
        final String staticLimitsName = "limits";
        String tooHighTemperatureCondition = new StringBuilder("data.")//
            .append(maxTemperatureSearch)
            .append(".aggregations.")//
            .append(aggregationNameMaxTemperature)//
            .append(".value > data.")//
            .append(staticLimitsName)//
            .append(".")//
            .append(maxTemperatureLimitName)//
            .toString();
        try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
            DocNode aggregation = DocNode.of(aggregationNameMaxTemperature, DocNode.of("max", DocNode.of("field", "temperature")));

            Watch watch = new WatchBuilder(watchId).triggerNow()
                .search(inputTemperatureIndex).size(0).aggregation(aggregation).as(maxTemperatureSearch)//
                .put(String.format("{\"%s\": %.2f}", maxTemperatureLimitName, temperatureLimit)).as(staticLimitsName)//
                .checkCondition(tooHighTemperatureCondition)//
                .then().index(outputAlarmIndex).name("createAlarm").build();
            String watchPath = createWatchPath(watchId);
            log.info("Predefined watch will be created using path '{}' and body '{}'", watchPath, watch.toJson());
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            WatchPointer watchPointer = new WatchPointer(watchPath);
            watchesToDelete.add(watchPointer);
            return watchPointer;
        } catch (Exception e) {
            throw new RuntimeException("Cannot define test watch", e);
        }
    }

    public WatchPointer defineSimpleTemperatureWatchWitDoubleActions(String watchId, String inputTemperatureIndex, String outputAlarmIndex,
        double temperatureLimit) {
        final String aggregationNameMaxTemperature = "max_temperature";
        final String maxTemperatureSearch = "max_temperature_search";
        final String maxTemperatureLimitName = "max_temperature";
        final String staticLimitsName = "limits";
        String tooHighTemperatureCondition = new StringBuilder("data.")//
            .append(maxTemperatureSearch)
            .append(".aggregations.")//
            .append(aggregationNameMaxTemperature)//
            .append(".value > data.")//
            .append(staticLimitsName)//
            .append(".")//
            .append(maxTemperatureLimitName)//
            .toString();
        try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
            DocNode aggregation = DocNode.of(aggregationNameMaxTemperature, DocNode.of("max", DocNode.of("field", "temperature")));

            Watch watch = new WatchBuilder(watchId).triggerNow()
                .search(inputTemperatureIndex).size(0).aggregation(aggregation).as(maxTemperatureSearch)//
                .put(String.format("{\"%s\": %.2f}", maxTemperatureLimitName, temperatureLimit)).as(staticLimitsName)//
                .checkCondition(tooHighTemperatureCondition)//
                .then().index(outputAlarmIndex).name(ACTION_CREATE_ALARM_ONE)
                .and().index(outputAlarmIndex).name(ACTION_CREATE_ALARM_TWO)
                .build();
            String watchPath = createWatchPath(watchId);
            log.info("Predefined watch will be created using path '{}' and body '{}'", watchPath, watch.toJson());
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            WatchPointer watchPointer = new WatchPointer(watchPath);
            watchesToDelete.add(watchPointer);
            return watchPointer;
        } catch (Exception e) {
            throw new RuntimeException("Cannot define test watch", e);
        }
    }

    public WatchPointer defineSimpleTemperatureWatchWitDoubleActionsAndVariousSeverity(String watchId, String inputTemperatureIndex, String outputAlarmIndex,
        double temperatureLimit) {
        final String aggregationNameMaxTemperature = "max_temperature";
        final String maxTemperatureSearch = "max_temperature_search";
        final String maxTemperatureLimitName = "max_temperature";
        final String staticLimitsName = "limits";
        String tooHighTemperatureCondition = new StringBuilder("data.")//
            .append(maxTemperatureSearch)
            .append(".aggregations.")//
            .append(aggregationNameMaxTemperature)//
            .append(".value > data.")//
            .append(staticLimitsName)//
            .append(".")//
            .append(maxTemperatureLimitName)//
            .toString();
        try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
            DocNode aggregation = DocNode.of(aggregationNameMaxTemperature, DocNode.of("max", DocNode.of("field", "temperature")));

            Watch watch = new WatchBuilder(watchId).triggerNow()
                .search(inputTemperatureIndex).size(0).aggregation(aggregation).as(maxTemperatureSearch)//
                .put(String.format("{\"%s\": %.2f}", maxTemperatureLimitName, temperatureLimit)).as(staticLimitsName)//
                .checkCondition(tooHighTemperatureCondition)//
                .consider("data." + maxTemperatureSearch + ".aggregations." + aggregationNameMaxTemperature + ".value")//
                .greaterOrEqual(0).as(NONE)//
                .greaterOrEqual(3).as(INFO)//
                .greaterOrEqual(7).as(WARNING)//
                .greaterOrEqual(10).as(ERROR)//
                .greaterOrEqual(15).as(CRITICAL)//
                .then().when(ERROR, WARNING).index(outputAlarmIndex).name(ACTION_CREATE_ALARM_ONE)//
                .and().when(CRITICAL).index(outputAlarmIndex).name(ACTION_CREATE_ALARM_TWO)//
                .build();
            String watchPath = createWatchPath(watchId);
            log.info("Predefined watch will be created using path '{}' and body '{}'", watchPath, watch.toJson());
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            WatchPointer watchPointer = new WatchPointer(watchPath);
            watchesToDelete.add(watchPointer);
            return watchPointer;
        } catch (Exception e) {
            throw new RuntimeException("Cannot define test watch", e);
        }
    }

    private String createWatchPath(String watchId) {
        return "/_signals/watch/" + tenant + "/" + watchId;
    }

    public void deleteWatches() {
        for(WatchPointer watchPointer : watchesToDelete) {
            try {
                watchPointer.delete();
                log.info("Watch '{}' deleted.", watchPointer);
            } catch (Exception ex) {
                throw new RuntimeException("Cannot delete watch " + watchPointer, ex);
            }
        }
        watchesToDelete.clear();
    }

    public long getCountOfDocuments(String index) throws InterruptedException, ExecutionException {
        try(Client client = localCluster.getPrivilegedInternalNodeClient()) {
            SearchRequest request = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            request.source(searchSourceBuilder);

            SearchResponse response = client.search(request).get();

            return response.getHits().getTotalHits().value;
        }
    }
    public long countWatchStatusWithAvailableStatusCode(String watchStateIndexName) {
        try(Client client = localCluster.getPrivilegedInternalNodeClient()) {
            SearchRequest request = new SearchRequest(watchStateIndexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.existsQuery("last_status.code"));
            request.source(searchSourceBuilder);

            SearchResponse response = client.search(request).get();

            return response.getHits().getTotalHits().value;
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException("Cannot count watch states with available status code.", e);
        }
    }

    public class WatchPointer {
        private final String watchPath;

        private WatchPointer(String watchPath) {
            this.watchPath = Objects.requireNonNull(watchPath);
        }

        public void delete() {
            try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
                HttpResponse response = restClient.delete(watchPath);
                log.info("Delete '{}' watch response code '{}' and body '{}'.", watchPath, response.getStatusCode(), response.getBody());
                Awaitility.await().until(this::notExist);
            } catch (Exception e) {
                throw new RuntimeException("Cannot delete watch", e);
            }
        }

        public boolean exists() {
            try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
                HttpResponse response = restClient.get(watchPath);
                int statusCode = response.getStatusCode();
                switch (statusCode) {
                    case 200:
                        return true;
                    case 404:
                        return false;
                    default:
                        throw new RuntimeException("Cannot check if watch " + watchPath + " exists.");
                }
            } catch (Exception e) {
                throw new RuntimeException("Cannot check if watch " + watchPath + " exists." , e);
            }
        }

        public boolean notExist() {
            return ! exists();
        }

        @Override
        public String toString() {
            return "WatchPointer{" + "watchPath='" + watchPath + '\'' + '}';
        }
    }

    public WatchPointer defineTemperatureWatchWithActionOnCriticalSeverity(String watchId, String inputTemperatureIndex, String outputAlarmIndex,
        double temperatureLimit) {
        final String aggregationNameMaxTemperature = "max_temperature";
        final String maxTemperatureSearch = "max_temperature_search";
        final String maxTemperatureLimitName = "max_temperature";
        final String staticLimitsName = "limits";
        String tooHighTemperatureCondition = new StringBuilder("data.")//
            .append(maxTemperatureSearch)
            .append(".aggregations.")//
            .append(aggregationNameMaxTemperature)//
            .append(".value > data.")//
            .append(staticLimitsName)//
            .append(".")//
            .append(maxTemperatureLimitName)//
            .toString();
        try (GenericRestClient restClient = localCluster.getRestClient(watchUser)){
            DocNode aggregation = DocNode.of(aggregationNameMaxTemperature, DocNode.of("max", DocNode.of("field", "temperature")));

            Watch watch = new WatchBuilder(watchId).triggerNow()
                .search(inputTemperatureIndex).size(0).aggregation(aggregation).as(maxTemperatureSearch)//
                .put(String.format("{\"%s\": %.2f}", maxTemperatureLimitName, temperatureLimit)).as(staticLimitsName)//
                .checkCondition(tooHighTemperatureCondition)//
                .consider("data." + maxTemperatureSearch + ".aggregations." + aggregationNameMaxTemperature + ".value")//
                .greaterOrEqual(0).as(NONE)//
                .greaterOrEqual(3).as(INFO)//
                .greaterOrEqual(10).as(ERROR)//
                .greaterOrEqual(15).as(CRITICAL)//
                .when(CRITICAL).index(outputAlarmIndex).refreshPolicy(IMMEDIATE).name("createAlarm").build();
            String watchPath = createWatchPath(watchId);
            log.info("Predefined watch will be created using path '{}' and body '{}'", watchPath, watch.toJson());
            HttpResponse response = restClient.putJson(watchPath, watch);
            assertThat(response.getStatusCode(), equalTo(HttpStatus.SC_CREATED));
            WatchPointer watchPointer = new WatchPointer(watchPath);
            watchesToDelete.add(watchPointer);
            return watchPointer;
        } catch (Exception e) {
            throw new RuntimeException("Cannot define test watch", e);
        }
    }
}
