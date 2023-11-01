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

package com.floragunn.searchguard.configuration;

/**
 * Functional interface to let components listen to Search Guard configuration changes. Instances are registered with configurationRepository.subscribeOnChange().
 */
@FunctionalInterface
public interface ConfigurationChangeListener {

    /**
     * This method is called whenever the configuration changes. 
     * 
     * NOTE: This method is executed on a thread from the management thread pool. This means that you should not perform blocking operations on this thread. 
     * Either use async operations or move blocking operations to the generic thread pool.
     * 
     * @param configMap The updated configuration. Note: The map will only contain the config types that were changed. Unchanged types are not present here.
     */
    void onChange(ConfigMap configMap);
}
