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
package com.floragunn.searchsupport.jobs.config.schedule.elements;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public abstract class TriggerFactory<T extends HumanReadableCronTrigger<T>> {
    public abstract T create(DocNode jsonNode, TimeZone timeZone) throws ConfigValidationException;

    public abstract String getType();

    public static final List<TriggerFactory<?>> FACTORIES = Arrays.asList(DailyTrigger.FACTORY, HourlyTrigger.FACTORY, MonthlyTrigger.FACTORY,
            WeeklyTrigger.FACTORY);
}
