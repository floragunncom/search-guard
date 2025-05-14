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

package com.floragunn.searchguard.authtoken.update;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;

import com.floragunn.searchguard.authtoken.AuthToken;

public class PushAuthTokenUpdateRequest extends BaseNodesRequest {

    private AuthToken updatedToken;
    private UpdateType updateType;
    private long newHash;

    public PushAuthTokenUpdateRequest(AuthToken updatedToken, UpdateType updateType, long newHash) {
        super(new String[0]);
        this.updatedToken = updatedToken;
        this.updateType = updateType;
        this.newHash = newHash;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static enum UpdateType {
        NEW, REVOKED
    }

    public AuthToken getUpdatedToken() {
        return updatedToken;
    }

    public UpdateType getUpdateType() {
        return updateType;
    }

    @Override
    public String toString() {
        return "PushAuthTokenUpdateRequest [updatedToken=" + updatedToken + ", updateType=" + updateType + "]";
    }

    public long getNewHash() {
        return newHash;
    }
}
