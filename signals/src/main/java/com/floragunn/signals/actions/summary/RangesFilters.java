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

package com.floragunn.signals.actions.summary;

import com.floragunn.signals.actions.summary.WatchFilter.Range;

public class RangesFilters {

    private final Range levelNumericRange;
    private final Range actionsCheckedRange;
    private final Range actionsTriggeredRange;
    private final Range actionsExecutionRange;

    public RangesFilters(Range levelNumericRange, Range actionsCheckedRange, Range actionsTriggeredRange, Range actionsExecutionRange) {
        this.levelNumericRange = levelNumericRange;
        this.actionsCheckedRange = actionsCheckedRange;
        this.actionsTriggeredRange = actionsTriggeredRange;
        this.actionsExecutionRange = actionsExecutionRange;
    }

    public Range getLevelNumericRange() {
        return levelNumericRange;
    }

    public Range getActionsCheckedRange() {
        return actionsCheckedRange;
    }

    public Range getActionsTriggeredRange() {
        return actionsTriggeredRange;
    }

    public Range getActionsExecutionRange() {
        return actionsExecutionRange;
    }

    @Override public String toString() {
        return "RangesFilters{" + "levelNumericRange=" + levelNumericRange + ", actionsCheckedRange=" + actionsCheckedRange + ", actionsTriggeredRange=" + actionsTriggeredRange + ", actionsExecutionRange=" + actionsExecutionRange + '}';
    }
}
