/*
 * Copyright 2020-2022 floragunn GmbH
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
package com.floragunn.searchguard;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchguard.configuration.ConfigurationRepository;

public interface TypedComponent {
    String getType();

    public interface Info<ComponentType> {
        Class<ComponentType> getType();

        String getName();

        Factory<ComponentType> getFactory();

    }

    @FunctionalInterface
    public interface Factory<ComponentType> {
        ComponentType create(DocNode config, ConfigurationRepository.Context context) throws ConfigValidationException;
    }
}
