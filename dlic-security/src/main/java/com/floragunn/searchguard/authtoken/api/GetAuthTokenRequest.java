/*
  * Copyright 2020 by floragunn GmbH - All rights reserved
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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class GetAuthTokenRequest extends ActionRequest {

    private String authTokenId;

    public GetAuthTokenRequest() {
        super();
    }

    public GetAuthTokenRequest(String authTokenId) {
        super();
        this.authTokenId = authTokenId;

    }

    public GetAuthTokenRequest(StreamInput in) throws IOException {
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
