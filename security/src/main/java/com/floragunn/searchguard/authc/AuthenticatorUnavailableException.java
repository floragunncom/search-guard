/*
 * Copyright 2016-2022 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.authc;

import java.util.LinkedHashMap;
import java.util.Map;

import com.floragunn.fluent.collections.ImmutableMap;

public class AuthenticatorUnavailableException extends Exception {
    private static final long serialVersionUID = -7007025852090301416L;
    private ImmutableMap<String, Object> details;
    private String messageTitle;
    private String messageBody;

    public AuthenticatorUnavailableException() {
        super();
    }

    public AuthenticatorUnavailableException(String messageTitle, Throwable cause) {
        super(messageTitle + "\n" + cause.toString(), cause);
        this.messageTitle = messageTitle;
        this.messageBody = cause.toString();
    }

    public AuthenticatorUnavailableException(String messageTitle, String messageBody, Throwable cause) {
        super(messageTitle + "\n" + messageBody, cause);
        this.messageTitle = messageTitle;
        this.messageBody = messageBody;
    }

    public AuthenticatorUnavailableException(String messageTitle, String messageBody) {
        super(messageTitle + "\n" + messageBody);
        this.messageTitle = messageTitle;
        this.messageBody = messageBody;
    }

    public AuthenticatorUnavailableException details(Map<String, Object> details) {
        this.details = ImmutableMap.of(details);
        return this;
    }

    public AuthenticatorUnavailableException details(String key, Object value, Object... moreDetails) {
        if (moreDetails == null || moreDetails.length == 0) {
            this.details = ImmutableMap.of(key, convertToSimpleObject(value));
        } else {
            Map<String, Object> details = new LinkedHashMap<>(moreDetails.length + 1);
            details.put(key, value);

            for (int i = 0; i < moreDetails.length; i += 2) {
                details.put(String.valueOf(moreDetails[i]), convertToSimpleObject(moreDetails[i + 1]));
            }

            this.details = ImmutableMap.of(details);
        }

        return this;
    }

    public ImmutableMap<String, Object> getDetails() {
        return details;
    }

    public String getMessageTitle() {
        return messageTitle;
    }

    public String getMessageBody() {
        return messageBody;
    }
    
    private Object convertToSimpleObject(Object o) {        
        if (o instanceof Number || o instanceof Boolean || o instanceof String || o == null) {
            return o;
        } else {
            return o.toString();
        }
    }

}
