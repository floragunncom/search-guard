/*
 * Copyright 2015-2017 floragunn GmbH
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

package com.floragunn.searchguard.license.legacy;

import org.elasticsearch.action.ActionType;

/**
 * This license info action is still in use by the Search Guard Kibana plugin. 
 * For that reason, removal is a bit more complicated. First Kibana plugin needs to be changed 
 * to use the non-deprecated action. However, then it is still not possible to remove this action, as a newer Search Guard version
 * might still be used by an older Kibana plugin. Only as soon as it is not possible that a Kibana plugin which depends on this API
 * might run with a certain SG version, this API can be removed.
 */
@Deprecated
public class LicenseInfoAction extends ActionType<LicenseInfoResponse> {

    public static final LicenseInfoAction INSTANCE = new LicenseInfoAction();
    public static final String NAME = "cluster:admin/searchguard/license/info";

    protected LicenseInfoAction() {
        super(NAME);
    }
}
