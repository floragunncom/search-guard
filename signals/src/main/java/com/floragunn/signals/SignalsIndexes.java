package com.floragunn.signals;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;

import com.floragunn.searchguard.SearchGuardPlugin.ProtectedIndices;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.state.WatchState;

public class SignalsIndexes {
    private final static Logger log = LogManager.getLogger(SignalsIndexes.class);

    private final SignalsSettings settings;
    private final Client client;

    SignalsIndexes(SignalsSettings settings, Client client) {
        this.settings = settings;
        this.client = client;
    }

    void protectIndexes(ProtectedIndices protectedIndices) {
        protectedIndices.add(this.settings.getStaticSettings().getIndexNames().getWatches());
        protectedIndices.add(this.settings.getStaticSettings().getIndexNames().getWatchesState());
        protectedIndices.add(this.settings.getStaticSettings().getIndexNames().getWatchesTriggerState());
        protectedIndices.add(this.settings.getStaticSettings().getIndexNames().getAccounts());
        protectedIndices.add(this.settings.getStaticSettings().getIndexNames().getSettings());
    }

    private void install(ClusterState clusterState) {
        createConfigIndex(settings.getStaticSettings().getIndexNames().getWatches(), clusterState, Watch.getIndexMapping());
        createConfigIndex(settings.getStaticSettings().getIndexNames().getAccounts(), clusterState, null);
        createConfigIndex(settings.getStaticSettings().getIndexNames().getSettings(), clusterState, null);
        createConfigIndex(settings.getStaticSettings().getIndexNames().getWatchesState(), clusterState, WatchState.getIndexMapping());
    }

    private void createConfigIndex(String name, ClusterState clusterState, Map<String, Object> mapping) {
        CreateIndexRequest request = new CreateIndexRequest(name);

        if (mapping != null) {
            request.mapping("_doc", mapping);
        }

        create(request, clusterState);
    }

    private void create(CreateIndexRequest request, ClusterState clusterState) {
        String name = request.index();

        try {

            if (clusterState.metaData().getIndices().containsKey(name)) {
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Creating index " + request.index() + ":\n" + Strings.toString(request, true, true));
            }

            client.admin().indices().create(request, new ActionListener<CreateIndexResponse>() {

                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (log.isDebugEnabled()) {
                        log.debug("Created index " + name + ": " + response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ResourceAlreadyExistsException) {
                        // ignore
                        return;
                    } else {
                        log.error("Error while creating index " + name, e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Error while creating index " + name, e);
        }
    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            ClusterState clusterState = event.state();

            if (clusterState.getNodes().getLocalNode().isMasterNode()) {
                install(clusterState);
            }
        }
    };

    public ClusterStateListener getClusterStateListener() {
        return clusterStateListener;
    }

}
