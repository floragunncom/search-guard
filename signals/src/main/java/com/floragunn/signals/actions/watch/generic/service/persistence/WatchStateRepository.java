package com.floragunn.signals.actions.watch.generic.service.persistence;

import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchguard.support.PrivilegedConfigClient;
import com.floragunn.signals.NoSuchTenantException;
import com.floragunn.signals.Signals;
import com.floragunn.signals.SignalsTenant;
import com.floragunn.signals.SignalsUnavailableException;
import com.floragunn.signals.watch.Watch;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;

import java.util.Objects;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class WatchStateRepository {

    private final PrivilegedConfigClient client;

    private final Signals signals;

    private final String indexName;

    public WatchStateRepository(PrivilegedConfigClient client, Signals signals) {
        this.client = Objects.requireNonNull(client, "Client is required.");
        this.signals = Objects.requireNonNull(signals, "Signals singleton is required");
        this.indexName = Objects.requireNonNull(signals.getSignalsSettings().getStaticSettings().getIndexNames().getWatchesState());
    }

    public void deleteInstanceState(String tenantName, String watchId, ImmutableList<String> instanceIds, ActionListener<BulkResponse> deleteListener) {
        Objects.requireNonNull(tenantName, "Tenant name is required");
        Objects.requireNonNull(watchId, "Watch id is required");
        Objects.requireNonNull(instanceIds, "Instance IDs are required");
        Objects.requireNonNull(deleteListener, "Delete listener is required");
        try {
            BulkRequest bulkRequest = new BulkRequest(indexName);
            bulkRequest.setRefreshPolicy(IMMEDIATE);
            SignalsTenant signalsTenant = signals.getTenant(tenantName);
            for(String instanceId : instanceIds) {
                String stateId = signalsTenant.getWatchIdForConfigIndex(Watch.createInstanceId(watchId, instanceId));
                DeleteRequest deleteRequest = new DeleteRequest(indexName, stateId);
                bulkRequest.add(deleteRequest);
            }
            client.bulk(bulkRequest, deleteListener);
        } catch (SignalsUnavailableException | NoSuchTenantException e) {
            throw new RuntimeException("Cannot retrieve signal tenant '" + tenantName + "'.", e);
        }
    }
}
