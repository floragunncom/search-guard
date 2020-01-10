/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
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

package com.floragunn.dlic.auth.http.saml;

public class SamlConfigException extends Exception {

    private static final long serialVersionUID = 6888715101647475455L;

    public SamlConfigException() {
        super();
    }

    public SamlConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public SamlConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public SamlConfigException(String message) {
        super(message);
    }

    public SamlConfigException(Throwable cause) {
        super(cause);
    }

}
