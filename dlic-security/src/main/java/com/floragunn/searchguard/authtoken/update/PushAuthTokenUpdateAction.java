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

import org.elasticsearch.action.ActionType;

public class PushAuthTokenUpdateAction extends ActionType<PushAuthTokenUpdateResponse> {

    public static final PushAuthTokenUpdateAction INSTANCE = new PushAuthTokenUpdateAction();
    public static final String NAME = "cluster:admin/searchguard/auth_token/update/push";

    protected PushAuthTokenUpdateAction() {
        super(NAME);
    }
}
