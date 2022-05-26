/*
 * Copyright 2015-2022 floragunn GmbH
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

package com.floragunn.searchguard.legacy;

import java.nio.file.Path;
import java.util.function.BiFunction;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.TypedComponent;

public class LegacyComponentFactory {
    public static <ComponentType> TypedComponent.Factory<ComponentType> adapt(BiFunction<Settings, Path, ComponentType> function) {
        return (config, context) -> {
            Settings.Builder settings = Settings.builder().loadFromMap(config);

            if (context.getStaticSettings() != null) {
                settings.put(context.getStaticSettings().getPlatformSettings());
            }

            return function.apply(settings.build(), context.getStaticSettings() != null ? context.getStaticSettings().getConfigPath() : null);
        };
    }
}
