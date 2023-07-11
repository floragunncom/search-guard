package com.floragunn.signals.watch.common;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.ValidationError;
import com.floragunn.fluent.collections.ImmutableList;

import java.util.Objects;

public class InstanceParser {

    public static final String FIELD_INSTANCES = "instances";
    private final ValidationErrors validationErrors;

    public InstanceParser(ValidationErrors validationErrors) {
        this.validationErrors = Objects.requireNonNull(validationErrors, "Validation errors are required");
    }

    public Instances parse(ValidatingDocNode node) {
        Objects.requireNonNull(node, "Validating json node must not be null to read watch instance parameters.");
        try {
            if (node.hasNonNull(FIELD_INSTANCES)) {
                DocNode instancesNode = node.get(FIELD_INSTANCES).asDocNode();
                if (instancesNode.hasNonNull(Instances.FIELD_ENABLED)) {
                    boolean enabled = instancesNode.getBoolean(Instances.FIELD_ENABLED);
                    ImmutableList<String> params = instancesNode.getListOfStrings(Instances.FIELD_PARAMS);
                    params = params == null ? ImmutableList.empty() : params;
                    params.stream() //
                        .filter(name -> !Instances.isValidParameterName(name)) //
                        .forEach(invalidName -> {
                            String message = "Instance parameter name '" + invalidName + "' is invalid.";
                            String path = "instances." + invalidName;
                            validationErrors.add(new ValidationError(path, message));
                        });
                    if((!params.isEmpty()) && (!enabled)) {
                        String message = "Only generic watch is allowed to define instance parameters";
                        validationErrors.add(new ValidationError(FIELD_INSTANCES + "." + Instances.FIELD_ENABLED, message));
                    }
                    return new Instances(enabled, params);
                } else {
                    validationErrors.add(new ValidationError("instances.enabled", "Attribute is missing"));
                    return Instances.EMPTY;
                }
            } else {
                return Instances.EMPTY;
            }
        } catch (ConfigValidationException e) {
            validationErrors.add(FIELD_INSTANCES, e);
            return Instances.EMPTY;
        }
    }
}
