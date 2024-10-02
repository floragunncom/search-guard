package com.floragunn.aim.policy.instance;

import com.floragunn.aim.AutomatedIndexManagementSettings;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class PolicyInstanceConfigSource implements Iterable<PolicyInstance.Config> {

    private final ClusterService clusterService;
    private final PolicyInstance.Config.Factory configFactory;

    public PolicyInstanceConfigSource(ClusterService clusterService, PolicyInstance.Config.Factory configFactory) {
        this.clusterService = clusterService;
        this.configFactory = configFactory;
    }

    private Iterator<PolicyInstance.Config> initIterator() {
        ArrayList<PolicyInstance.Config> result = new ArrayList<>();
        for (Map.Entry<String, IndexMetadata> entry : clusterService.state().metadata().indices().entrySet()) {
            if (Strings.isNullOrEmpty(entry.getValue().getSettings().get(AutomatedIndexManagementSettings.Static.POLICY_NAME_FIELD.name()))) {
                continue;
            }
            PolicyInstance.Config config = configFactory.create(entry.getKey(), entry.getValue().getSettings());
            if (config != null) {
                result.add(config);
            }
        }
        return result.iterator();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<PolicyInstance.Config> iterator() {
        return new Iterator<>() {
            private Iterator<PolicyInstance.Config> iterator = null;

            @Override
            public boolean hasNext() {
                if (iterator == null) {
                    iterator = initIterator();
                }
                return iterator.hasNext();
            }

            @Override
            public PolicyInstance.Config next() {
                if (iterator == null) {
                    iterator = initIterator();
                }
                return iterator.next();
            }
        };
    }
}
