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

import java.util.Map;
import java.util.regex.Pattern;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;

import com.floragunn.searchguard.ssl.util.Utils;
import com.floragunn.searchguard.support.ConfigConstants;

public class InternalUsersValidator extends AbstractConfigurationValidator {

    public InternalUsersValidator(final RestRequest request, BytesReference ref, final Settings esSettings,
            Object... param) {
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
        if(!super.validate()) {
            return false;
        }

        final String regex = this.esSettings.get(ConfigConstants.SEARCHGUARD_RESTAPI_PASSWORD_VALIDATION_REGEX, null);

        if((request.method() == Method.PUT || request.method() == Method.PATCH ) 
                && regex != null 
                && !regex.isEmpty() 
                && this.content != null 
                && this.content.length() > 1) {
            try {
                final Map<String, Object> contentAsMap = XContentHelper.convertToMap(this.content, false, XContentType.JSON).v2();
                if(contentAsMap != null && contentAsMap.containsKey("password")) {
                    final String password = (String) contentAsMap.get("password");

                    if(password == null || password.isEmpty()) {
                        if(log.isDebugEnabled()) {
                            log.debug("Unable to validate password because no password is given");
                        }
                        return false;
                    }
                    
                    if(!regex.isEmpty() && !Pattern.compile("^"+regex+"$").matcher(password).matches()) {
                        if(log.isDebugEnabled()) {
                            log.debug("Regex does not match password");
                        }
                        this.errorType = ErrorType.INVALID_PASSWORD;
                        return false;
                    }

                    final String username = Utils.coalesce(request.param("name"), hasParams()?(String)param[0]:null);
                    
                    if(username == null || username.isEmpty()) {
                        if(log.isDebugEnabled()) {
                            log.debug("Unable to validate username because no user is given");
                        }
                        return false;
                    }

                    if(username.toLowerCase().equals(password.toLowerCase())) {
                        if(log.isDebugEnabled()) {
                            log.debug("Username must not match password");
                        }
                        this.errorType = ErrorType.INVALID_PASSWORD;
                        return false;
                    }
                }
            } catch (NotXContentException e) {
                //this.content is not valid json/yaml
                log.error("Invalid xContent: "+e,e);
                return false;
            }
        }
        return true;
    }
}
