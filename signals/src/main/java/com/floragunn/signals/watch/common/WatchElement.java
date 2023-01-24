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
package com.floragunn.signals.watch.common;

import com.floragunn.signals.execution.WatchExecutionContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.xcontent.ToXContentObject;

public abstract class WatchElement implements ToXContentObject {

    public Iterable<? extends WatchElement> getChildren() {
        return Collections.emptyList();
    }

    public <T> T getChildByNameAndType(Class<T> type, String name) {
        if (name == null) {
            return null;
        }

        for (WatchElement o : getChildren()) {
            if (o == null) {
                continue;
            }

            if (type.isAssignableFrom(o.getClass()) && name.equals(o.getName())) {
                return type.cast(o);
            }
        }

        return null;
    }

    public String getName() {
        return null;
    }

    protected String render(WatchExecutionContext ctx, TemplateScript.Factory script) {
        if (script != null) {
            return script.newInstance(ctx.getTemplateScriptParamsAsMap()).execute();
        } else {
            return null;
        }
    }

    protected List<String> render(WatchExecutionContext ctx, List<TemplateScript.Factory> list) {
        if (list == null || list.size() == 0) {
            return Collections.emptyList();
        } else {
            return list.stream().map((script) -> render(ctx, script)).collect(Collectors.toList());
        }
    }
}
