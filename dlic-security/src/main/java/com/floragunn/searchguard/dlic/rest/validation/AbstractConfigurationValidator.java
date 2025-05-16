/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard.dlic.rest.validation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocReader;
import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.codova.documents.Format;
import com.floragunn.codova.documents.UnexpectedDocumentStructureException;
import com.floragunn.searchguard.support.ConfigConstants;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public abstract class AbstractConfigurationValidator {

    JsonFactory factory = new JsonFactory();

    /* public for testing */
    public final static String INVALID_KEYS_KEY = "invalid_keys";

    /* public for testing */
    public final static String MISSING_MANDATORY_KEYS_KEY = "missing_mandatory_keys";

    /* public for testing */
    public final static String MISSING_MANDATORY_OR_KEYS_KEY = "specify_one_of";

    protected final Logger log = LogManager.getLogger(this.getClass());

    /**
     * Define the various keys for this validator
     */
    protected final Map<String, DataType> allowedKeys = new HashMap<>();

    protected final Set<String> mandatoryKeys = new HashSet<>();

    protected final Set<String> mandatoryOrKeys = new HashSet<>();

    protected final Map<String, String> wrongDatatypes = new HashMap<>();

    /**
     * Contain errorneous keys
     */
    protected final Set<String> missingMandatoryKeys = new HashSet<>();

    protected final Set<String> invalidKeys = new HashSet<>();

    protected final Set<String> missingMandatoryOrKeys = new HashSet<>();

    /**
     * The error type
     */
    protected ErrorType errorType = ErrorType.NONE;

    protected Exception lastException;

    /**
     * Behaviour regarding payload
     */
    protected boolean payloadMandatory = false;

    protected boolean payloadAllowed = true;

    protected final Method method;

    protected final BytesReference content;

    protected final Settings esSettings;

    protected final RestRequest request;

    protected final Object[] param;

    private DocNode contentAsNode;

    public AbstractConfigurationValidator(final RestRequest request, final BytesReference ref, final Settings esSettings, Object... param) {
        this.content = ref;
        this.method = request.method();
        this.esSettings = esSettings;
        this.request = request;
        this.param = param;
    }

    public DocNode getContentAsNode() {
        return contentAsNode;
    }

    /**
     * @return false if validation fails
     */
    public boolean validate() {
        // no payload for DELETE and GET requests
        if (method.equals(Method.DELETE) || method.equals(Method.GET)) {
            return true;
        }

        if (this.payloadMandatory && content.length() == 0) {
            this.errorType = ErrorType.PAYLOAD_MANDATORY;
            return false;
        }

        if (this.payloadMandatory && content.length() > 0) {

            try {
                if (DocReader.json().readObject(content.utf8ToString()).size() == 0) {
                    this.errorType = ErrorType.PAYLOAD_MANDATORY;
                    return false;
                }

            } catch (DocumentParseException | UnexpectedDocumentStructureException e) {
                log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), validationError(e));
                this.errorType = ErrorType.BODY_NOT_PARSEABLE;
                lastException = validationError(e);
                return false;
            }
        }

        if (!this.payloadAllowed && content.length() > 0) {
            this.errorType = ErrorType.PAYLOAD_NOT_ALLOWED;
            return false;
        }

        // try to parse payload
        Set<String> requested = new HashSet<String>();
        try {
            contentAsNode = DocNode.parse(Format.JSON).from(content.utf8ToString());
            
            if (contentAsNode == null || contentAsNode.isNull()) {
                this.errorType = ErrorType.BODY_NOT_PARSEABLE;
                return false;
             }
            
            requested.addAll(ImmutableList.copyOf(contentAsNode.keySet()));
        } catch (Exception e) {
            log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), e);
            this.errorType = ErrorType.BODY_NOT_PARSEABLE;
            lastException = e;
            return false;
        }

        // mandatory settings, one of ...
        if (Collections.disjoint(requested, mandatoryOrKeys)) {
            this.missingMandatoryOrKeys.addAll(mandatoryOrKeys);
        }

        // mandatory settings
        Set<String> mandatory = new HashSet<>(mandatoryKeys);
        mandatory.removeAll(requested);
        missingMandatoryKeys.addAll(mandatory);

        // invalid settings
        Set<String> allowed = new HashSet<>(allowedKeys.keySet());
        requested.removeAll(allowed);
        this.invalidKeys.addAll(requested);
        boolean valid = missingMandatoryKeys.isEmpty() && invalidKeys.isEmpty() && missingMandatoryOrKeys.isEmpty();
        if (!valid) {
            this.errorType = ErrorType.INVALID_CONFIGURATION;
        }

        // check types
        try {
            if (!checkDatatypes()) {
                this.errorType = ErrorType.WRONG_DATATYPE;
                return false;
            }
        } catch (Exception e) {
            log.error(ErrorType.BODY_NOT_PARSEABLE.toString(), e);
            this.errorType = ErrorType.BODY_NOT_PARSEABLE;
            lastException = e;
            return false;
        }

        return valid;
    }

    protected Exception validationError(Exception e) {
        return e;
    }

    private boolean checkDatatypes() throws Exception {
        String contentAsJson = XContentHelper.convertToJson(content, false, XContentType.JSON);
        try (JsonParser parser = factory.createParser(contentAsJson)) {
            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                if (token.equals(JsonToken.FIELD_NAME)) {
                    String currentName = parser.getCurrentName();
                    DataType dataType = allowedKeys.get(currentName);
                    if (dataType != null) {
                        JsonToken valueToken = parser.nextToken();
                        switch (dataType) {
                            case STRING:
                                if (!valueToken.equals(JsonToken.VALUE_STRING)) {
                                    wrongDatatypes.put(currentName, "String expected");
                                }
                                break;
                            case ARRAY:
                                if (!valueToken.equals(JsonToken.START_ARRAY) && !valueToken.equals(JsonToken.END_ARRAY)) {
                                    wrongDatatypes.put(currentName, "Array expected");
                                }
                                break;
                            case OBJECT:
                                if (!valueToken.equals(JsonToken.START_OBJECT) && !valueToken.equals(JsonToken.END_OBJECT)) {
                                    wrongDatatypes.put(currentName, "Object expected");
                                }
                                break;
                        }
                    }
                }
            }
            return wrongDatatypes.isEmpty();
        }
    }

    public XContentBuilder errorsAsXContent(RestChannel channel) {
        try {
            final XContentBuilder builder = channel.newErrorBuilder();
            builder.startObject();
            if (lastException != null) {
                builder.field("details", lastException.toString());
            }
            switch (this.errorType) {
                case INVALID_CONFIGURATION:
                    builder.field("status", "error");
                    builder.field("reason", ErrorType.INVALID_CONFIGURATION.getMessage());
                    addErrorMessage(builder, INVALID_KEYS_KEY, invalidKeys);
                    addErrorMessage(builder, MISSING_MANDATORY_KEYS_KEY, missingMandatoryKeys);
                    addErrorMessage(builder, MISSING_MANDATORY_OR_KEYS_KEY, missingMandatoryKeys);
                    break;
                case INVALID_PASSWORD:
                    builder.field("status", "error");
                    builder.field("reason", esSettings.get(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_ERROR_MESSAGE,
                            "Password does not match minimum criterias"));
                    break;
                case WRONG_DATATYPE:
                    builder.field("status", "error");
                    builder.field("reason", ErrorType.WRONG_DATATYPE.getMessage());
                    for (Entry<String, String> entry : wrongDatatypes.entrySet()) {
                        builder.field(entry.getKey(), entry.getValue());
                    }
                    break;
                default:
                    builder.field("status", "error");
                    builder.field("reason", errorType.getMessage());
            }
            builder.endObject();
            return builder;
        } catch (IOException ex) {
            log.error("Cannot build error settings", ex);
            return null;
        }
    }

    private void addErrorMessage(final XContentBuilder builder, final String message, final Set<String> keys) throws IOException {
        if (!keys.isEmpty()) {
            builder.startObject(message);
            builder.field("keys", Joiner.on(",").join(keys.toArray(new String[0])));
            builder.endObject();
        }
    }

    public enum DataType {
        STRING, ARRAY, OBJECT
    }

    public enum ErrorType {
        NONE("ok"), INVALID_CONFIGURATION("Invalid configuration"), INVALID_PASSWORD("Invalid password"), WRONG_DATATYPE("Wrong datatype"),
        BODY_NOT_PARSEABLE("Could not parse content of request."), PAYLOAD_NOT_ALLOWED("Request body not allowed for this action."),
        PAYLOAD_MANDATORY("Request body required for this action."), SG_NOT_INITIALIZED("Search Guard index not initialized (SG11)");

        private String message;

        ErrorType(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    protected final boolean hasParams() {
        return param != null && param.length > 0;
    }
}
