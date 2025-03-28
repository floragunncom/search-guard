/*
 * Copyright 2022 floragunn GmbH
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

import java.util.concurrent.CompletableFuture;

import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchsupport.cstate.metrics.Meter;

public interface UserInformationBackend {
    String getType();

    CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials userInformation, Meter meter) throws AuthenticatorUnavailableException;

    default CompletableFuture<AuthCredentials> getUserInformation(AuthCredentials userInformation, Meter meter, AuthenticationDebugLogger debug)
            throws AuthenticatorUnavailableException {
        return getUserInformation(userInformation, meter);
    }
}
