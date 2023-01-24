/*
 * Copyright 2021 floragunn GmbH
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

public interface LoginPrivileges {
    public static final String HTTP_AUTHORIZATION_HEADER = "cluster:admin:searchguard:login/http_authorization_header";
    public static final String TRANSPORT = "cluster:admin:searchguard:login/transport";
    public static final String SESSION = "cluster:admin:searchguard:login/session";
}
