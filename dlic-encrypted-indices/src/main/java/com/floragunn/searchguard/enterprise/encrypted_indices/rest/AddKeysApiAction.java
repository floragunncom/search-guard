package com.floragunn.searchguard.enterprise.encrypted_indices.rest;

import com.floragunn.searchguard.enterprise.encrypted_indices.EncryptedIndicesSettings;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.CryptoOperations;
import com.floragunn.searchguard.enterprise.encrypted_indices.crypto.IndexKeys;
import com.google.common.collect.ImmutableList;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;

public class AddKeysApiAction extends BaseRestHandler {

    final ClusterService clusterService;
    final ThreadContext threadContext;

    public AddKeysApiAction(ClusterService clusterService, ThreadContext threadContext) {
        this.clusterService = clusterService;
        this.threadContext = threadContext;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(POST, "/_searchguard/api/encrypted_indices/{index}/_add_keys"));
    }

    @Override
    public String getName() {
        return "Add Keys Action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");

        if(index == null) {

        }

        BytesReference content = request.requiredContent();

        return channel -> client.admin().indices().getSettings(
                new GetSettingsRequest()
                .indices(index)
                        .indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                        .includeDefaults(false),
                new ActionListener<GetSettingsResponse>() {
                    @Override
                    public void onResponse(GetSettingsResponse getSettingsResponse) {
                        try {
                            final Settings indexSettings = getSettingsResponse.getIndexToSettings().get(index);
                            if(!EncryptedIndicesSettings.INDEX_ENCRYPTION_ENABLED.getFrom(indexSettings).booleanValue()) {

                            }

                            final String ownerKey = EncryptedIndicesSettings.INDEX_ENCRYPTION_KEY.getFrom(indexSettings);
                            IndexKeys ik = new IndexKeys(clusterService, index, client, threadContext, CryptoOperations.parsePublicKey(ownerKey));

                            Map<String, Object> source = XContentHelper.convertToMap(content, false, request.getXContentType()).v2();

                            ik.addKey(CryptoOperations.parsePublicKey((String) source.get("public_key")), new RestStatusToXContentListener<>(channel, r -> null));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}