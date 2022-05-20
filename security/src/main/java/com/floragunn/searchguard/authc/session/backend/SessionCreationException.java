/*
 * Copyright 2020 by floragunn GmbH - All rights reserved
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

package com.floragunn.searchguard.authc.session.backend;

import org.opensearch.rest.RestStatus;

public class SessionCreationException extends Exception {

    private static final long serialVersionUID = -47600121877964762L;
    
    private RestStatus restStatus;

    public SessionCreationException(String message, RestStatus restStatus, Throwable cause) {
        super(message, cause);
        this.restStatus = restStatus;
    }

    public SessionCreationException(String message, RestStatus restStatus) {
        super(message);
        this.restStatus = restStatus;
    }
    
    
    public RestStatus getRestStatus() {
        return restStatus;
    }

    public void setRestStatus(RestStatus restStatus) {
        this.restStatus = restStatus;
    }



}
