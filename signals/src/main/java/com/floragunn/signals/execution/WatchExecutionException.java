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
package com.floragunn.signals.execution;

import com.floragunn.signals.watch.result.WatchLog;

public class WatchExecutionException extends Exception {
    private static final long serialVersionUID = 3454171384248735178L;

    private final WatchLog watchLog;

    public WatchExecutionException(String message, Throwable cause, WatchLog watchLog) {
        super(message, cause);
        this.watchLog = watchLog;
    }

    public WatchExecutionException(String message, WatchLog watchLog) {
        super(message);
        this.watchLog = watchLog;

    }

    public WatchExecutionException(Throwable cause, WatchLog watchLog) {
        super(cause);
        this.watchLog = watchLog;

    }

    public WatchLog getWatchLog() {
        return watchLog;
    }

}
