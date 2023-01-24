/*
  * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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

import com.floragunn.codova.documents.DocumentParseException;
import com.floragunn.searchguard.support.ConfigConstants;
import java.util.Map;
import java.util.regex.Pattern;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.xcontent.XContentType;

public class InternalUsersValidator extends AbstractConfigurationValidator {

    public InternalUsersValidator(final RestRequest request, BytesReference ref, final Settings esSettings, Object... param) {
        super(request, ref, esSettings, param);
        this.payloadMandatory = true;
        allowedKeys.put("hash", DataType.STRING);
        allowedKeys.put("password", DataType.STRING);
        allowedKeys.put("backend_roles", DataType.ARRAY);
        allowedKeys.put("attributes", DataType.OBJECT);
        allowedKeys.put("description", DataType.STRING);
        allowedKeys.put("search_guard_roles", DataType.ARRAY);
    }

    @Override
    public boolean validate() {
        if (!super.validate()) {
            return false;
        }

        final String regex = this.esSettings.get(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_REGEX, null);

        if ((request.method() == Method.PUT || request.method() == Method.PATCH) && regex != null && !regex.isEmpty() && this.content != null
                && this.content.length() > 1) {
            try {
                final Map<String, Object> contentAsMap = XContentHelper.convertToMap(this.content, false, XContentType.JSON).v2();
                if (contentAsMap != null && contentAsMap.containsKey("password")) {
                    final String password = (String) contentAsMap.get("password");

                    if (password == null || password.isEmpty()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Unable to validate password because no password is given");
                        }
                        return false;
                    }

                    String username = request.param("name");

                    if (username == null && hasParams()) {
                        username = (String) param[0];
                    }

                    if (username == null || username.isEmpty()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Unable to validate username because no user is given");
                        }
                        return false;
                    }

                    ErrorType error = validatePassword(username, password, esSettings);

                    if (error != null) {
                        this.errorType = error;
                        return false;
                    }
                }
            } catch (NotXContentException e) {
                //this.content is not valid json/yaml
                log.error("Invalid xContent: " + e, e);
                return false;
            }
        }
        return true;
    }

    public static ErrorType validatePassword(String username, String password, Settings esSettings) {

        if (password == null || password.isEmpty()) {
            return null;
        }

        String regex = esSettings.get(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_REGEX, null);

        if (regex == null || regex.isEmpty()) {
            return null;
        }

        if (!Pattern.compile("^" + regex + "$").matcher(password).matches()) {
            return ErrorType.INVALID_PASSWORD;
        }

        if (username.toLowerCase().equals(password.toLowerCase())) {
            return ErrorType.INVALID_PASSWORD;
        }

        return null;
    }

    @Override
    protected Exception validationError(Exception e) {
        if (e instanceof DocumentParseException) {
            return new SensitiveDataException("Passed User object is invalid");
        }
        return e;
    }

    private static class SensitiveDataException extends Exception {

        private static final long serialVersionUID = 7279592878585611145L;

        public SensitiveDataException(String message) {
            super(message);
        }
    }
}
