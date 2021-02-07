/*
 * Copyright 2020-2021 floragunn GmbH
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

package com.floragunn.searchguard.modules;

public class NoSuchComponentException extends Exception {

    private static final long serialVersionUID = 3631079566578234957L;

    private final String componentId;
    
    public NoSuchComponentException(String componentId) {
        super("No such component: " + componentId);
        this.componentId = componentId;
    }

    public NoSuchComponentException(String componentId, Throwable cause) {
        super("No such component: " + componentId, cause);
        this.componentId = componentId;
    }

    public String getComponentId() {
        return componentId;
    }

}
