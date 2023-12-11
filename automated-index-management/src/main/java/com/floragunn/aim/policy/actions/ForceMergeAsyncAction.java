package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.conditions.ForceMergeDoneCondition;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.rest.RestStatus;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;

public final class ForceMergeAsyncAction extends Action.Async<ForceMergeDoneCondition> {
    private static final Logger LOG = LogManager.getLogger(ForceMergeAsyncAction.class);
    public static final String TYPE = "force_merge";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            int segments = node.get(SEGMENTS_FIELD).required().asInt();
            if (validationContext != null && segments < 1) {
                errors.add(new InvalidAttributeValue(SEGMENTS_FIELD, segments, "value > 0", node));
            }
            return new ForceMergeAsyncAction(segments);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
            typeValidator.validateIndexBlocked();
            typeValidator.validateOnlyOnceInPolicy();
        }
    };
    public static final String SEGMENTS_FIELD = "segments";

    private final int segments;

    public ForceMergeAsyncAction(int segments) {
        this.segments = segments;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        GetSettingsRequest settingsRequest = new GetSettingsRequest().indices(index).names(SETTING_BLOCKS_WRITE);
        GetSettingsResponse settingsResponse = executionContext.getClient().admin().indices().getSettings(settingsRequest).actionGet();
        if (!"true".equals(settingsResponse.getSetting(index, SETTING_BLOCKS_WRITE))) {
            throw new IllegalStateException("Index was not set to read only");
        }
        ForceMergeRequest request = new ForceMergeRequest(index).maxNumSegments(segments);
        ForceMergeResponse response = executionContext.getClient().admin().indices().forceMerge(request).actionGet();
        if (response.getStatus() == RestStatus.OK) {
            LOG.debug("Starting force merge on index '" + index + "' successful.");
        } else {
            throw new IllegalStateException("Starting force merge failed. Response was not ok");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ForceMergeAsyncAction)) {
            return false;
        }
        ForceMergeAsyncAction forceMergeAsyncAction = (ForceMergeAsyncAction) other;
        return forceMergeAsyncAction.segments == segments;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        return ImmutableMap.of(SEGMENTS_FIELD, segments);
    }

    @Override
    public ForceMergeDoneCondition createCondition() {
        return new ForceMergeDoneCondition(segments);
    }
}
