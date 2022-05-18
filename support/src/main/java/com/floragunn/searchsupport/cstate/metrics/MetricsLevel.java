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
package com.floragunn.searchsupport.cstate.metrics;

public enum MetricsLevel {
    NONE(0), BASIC(1), DETAILED(2);

    private final int level;

    MetricsLevel(int level) {
        this.level = level;
    }

    public boolean basicEnabled() {
        return level >= BASIC.level;
    }

    public boolean detailedEnabled() {
        return level >= DETAILED.level;
    }
}
