package com.floragunn.aim.policy.conditions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.fluent.collections.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.engine.SegmentsStats;

public final class ForceMergeDoneCondition extends Condition.Async {
    private static final Logger LOG = LogManager.getLogger(ForceMergeDoneCondition.class);
    public static final String TYPE = "force_merge_done";
    public static final String STEP_NAME = "awaiting_force_merge";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Condition parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            int segments = node.get(SEGMENTS_FIELD).required().asInt();
            return new ForceMergeDoneCondition(segments);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateNotConfigurable();
        }
    };
    public static final String SEGMENTS_FIELD = "segments";

    private final int segments;

    public ForceMergeDoneCondition(int segments) {
        this.segments = segments;
    }

    @Override
    public boolean execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        IndicesStatsResponse response = getIndexStats(index, executionContext);
        int mergingShards = 0;
        for (ShardStats shard : response.getShards()) {
            SegmentsStats segmentsStats = shard.getStats().getSegments();
            if (segmentsStats == null) {
                LOG.warn("Index '{}' had null segments waiting for force merge.", index);
            } else if (segments < shard.getStats().getSegments().getCount()) {
                mergingShards++;
            }
        }
        if (mergingShards == 0) {
            return true;
        } else {
            //TODO: Implement timeout?
            return false;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ForceMergeDoneCondition)) {
            return false;
        }
        ForceMergeDoneCondition forceMergeDoneCondition = (ForceMergeDoneCondition) other;
        return forceMergeDoneCondition.segments == segments;
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
    public String getStepName() {
        return STEP_NAME;
    }
}
