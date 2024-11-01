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

package com.floragunn.searchguard.action.whoami;

import org.elasticsearch.action.ActionType;

public class WhoAmIAction extends ActionType<WhoAmIResponse> {

    public static final WhoAmIAction INSTANCE = new WhoAmIAction();
    public static final String NAME = "cluster:admin/searchguard/whoami";

    protected WhoAmIAction() {
        super(NAME, WhoAmIResponse::new);
    }
}
