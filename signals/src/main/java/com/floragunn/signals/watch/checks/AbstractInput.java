/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.watch.checks;

import com.floragunn.signals.execution.WatchExecutionContext;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.support.NestedValueMap.Path;
import com.google.common.base.Strings;
import java.util.Map;

public abstract class AbstractInput extends Check {
    protected final String target;

    public AbstractInput(String name, String target) {
        super(name);
        this.target = target;
    }

    protected void setResult(WatchExecutionContext ctx, Object result) {

        NestedValueMap data = ctx.getContextData().getData();

        if (Strings.isNullOrEmpty(target) || "_top".equals(target)) {
            data.clear();

            if (result instanceof Map) {
                data.putAllFromAnyMap((Map<?, ?>) result);
            } else {
                data.put("_value", result);
            }
        } else {
            data.put(Path.parse(target), result);
        }
    }
}
