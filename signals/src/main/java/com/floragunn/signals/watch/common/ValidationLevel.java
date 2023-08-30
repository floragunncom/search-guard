/*
 * Copyright 2020-2023 floragunn GmbH
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
package com.floragunn.signals.watch.common;

import com.floragunn.signals.truststore.service.TrustManagerRegistry;

/**
 * Acts as boolean type with more expressive names. <code>STRICT</code> for <code>true</code> and <code>LENIENT</code> for
 * <code>false</code>.
 * 
 * @see TlsConfig#TlsConfig(TrustManagerRegistry, ValidationLevel) 
 */
public enum ValidationLevel {
    STRICT(true), LENIENT(false);

    private final boolean strictValidation;

    ValidationLevel(boolean strictValidation) {
        this.strictValidation = strictValidation;
    }

    boolean isStrictValidation() {
        return strictValidation;
    }
}
