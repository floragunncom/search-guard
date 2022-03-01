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

package com.floragunn.searchguard.authtoken.api;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class RevokeAuthTokenRequest extends ActionRequest {

    private String authTokenId;

    public RevokeAuthTokenRequest() {
        super();
    }

    public RevokeAuthTokenRequest(String authTokenId) {
        super();
        this.authTokenId = authTokenId;

    }

    public RevokeAuthTokenRequest(StreamInput in) throws IOException {
        super(in);
        this.authTokenId = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(authTokenId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getAuthTokenId() {
        return authTokenId;
    }

}
