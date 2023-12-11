package com.floragunn.aim.policy.actions;

import com.floragunn.aim.policy.Policy;
import com.floragunn.aim.policy.instance.PolicyInstance;
import com.floragunn.aim.policy.instance.PolicyInstanceState;
import com.floragunn.aim.support.ParsingSupport;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

public final class AllocationAction extends Action {
    public static final String TYPE = "allocation";
    public static final ValidatingParser VALIDATING_PARSER = new ValidatingParser() {
        @Override
        public Action parse(ValidatingDocNode node, ValidationErrors errors, Policy.ValidationContext validationContext) {
            Map<String, String> empty = ImmutableMap.empty();
            Map<String, String> require = node.get(REQUIRE_FIELD).withDefault(empty).by(ParsingSupport::stringMapParser);
            Map<String, String> include = node.get(INCLUDE_FIELD).withDefault(empty).by(ParsingSupport::stringMapParser);
            Map<String, String> exclude = node.get(EXCLUDE_FIELD).withDefault(empty).by(ParsingSupport::stringMapParser);
            if (validationContext != null && require.isEmpty() && include.isEmpty() && exclude.isEmpty()) {
                errors.add(
                        new ValidationError(REQUIRE_FIELD + "|" + INCLUDE_FIELD + "|" + EXCLUDE_FIELD, "At least one attribute must be configured."));
            }
            return new AllocationAction(require, include, exclude);
        }

        @Override
        public void validateType(Validator.TypeValidator typeValidator) {
            typeValidator.validateIndexNotDeleted();
            typeValidator.validateNoReadOnlyActionBeforeInState();
        }
    };
    public static final String REQUIRE_FIELD = "require";
    public static final String INCLUDE_FIELD = "include";
    public static final String EXCLUDE_FIELD = "exclude";

    private static final String SETTING_PREFIX = "index.routing.allocation.";

    private final Map<String, String> require;
    private final Map<String, String> include;
    private final Map<String, String> exclude;

    public AllocationAction(Map<String, String> require, Map<String, String> include, Map<String, String> exclude) {
        this.require = require;
        this.include = include;
        this.exclude = exclude;
    }

    @Override
    public void execute(String index, PolicyInstance.ExecutionContext executionContext, PolicyInstanceState state) throws Exception {
        Settings.Builder builder = Settings.builder();
        require.forEach((key, value) -> builder.put(SETTING_PREFIX + REQUIRE_FIELD + "." + key, value));
        include.forEach((key, value) -> builder.put(SETTING_PREFIX + INCLUDE_FIELD + "." + key, value));
        exclude.forEach((key, value) -> builder.put(SETTING_PREFIX + EXCLUDE_FIELD + "." + key, value));
        setIndexSetting(index, executionContext, builder);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AllocationAction)) {
            return false;
        }
        AllocationAction allocationAction = (AllocationAction) other;
        if (!allocationAction.require.equals(require)) {
            return false;
        }
        if (!allocationAction.include.equals(include)) {
            return false;
        }
        return allocationAction.exclude.equals(exclude);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ImmutableMap<String, Object> configToBasicMap() {
        Map<String, Object> map = new HashMap<>();
        if (!require.isEmpty()) {
            map.put(REQUIRE_FIELD, require);
        }
        if (!include.isEmpty()) {
            map.put(INCLUDE_FIELD, include);
        }
        if (!exclude.isEmpty()) {
            map.put(EXCLUDE_FIELD, exclude);
        }
        return ImmutableMap.of(map);
    }
}
