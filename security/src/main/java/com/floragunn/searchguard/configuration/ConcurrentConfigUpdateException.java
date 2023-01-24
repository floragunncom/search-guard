/*
 * Copyright 2021-2022 floragunn GmbH
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
package com.floragunn.searchguard.configuration;

import java.util.Map;

public class ConcurrentConfigUpdateException extends Exception {

    private static final long serialVersionUID = -3653303121589781438L;
    private Map<CType<?>, ConfigurationRepository.ConfigUpdateResult> updateResult;

    public ConcurrentConfigUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConcurrentConfigUpdateException(String message) {
        super(message);
    }

    public ConcurrentConfigUpdateException(Throwable cause) {
        super(cause);
    }

    public Map<CType<?>, ConfigurationRepository.ConfigUpdateResult> getUpdateResult() {
        return updateResult;
    }

    ConcurrentConfigUpdateException updateResult(Map<CType<?>, ConfigurationRepository.ConfigUpdateResult> updateResult) {
        this.updateResult = updateResult;
        return this;
    }
}
