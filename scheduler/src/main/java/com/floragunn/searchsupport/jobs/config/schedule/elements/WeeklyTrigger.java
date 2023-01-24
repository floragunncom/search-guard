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
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import java.io.IOException;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.elasticsearch.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.DateBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CronTriggerImpl;

public class WeeklyTrigger extends HumanReadableCronTrigger<WeeklyTrigger> {

    private static final long serialVersionUID = -8707981523995856355L;

    private List<DayOfWeek> on;
    private List<TimeOfDay> at;

    public WeeklyTrigger(List<DayOfWeek> on, List<TimeOfDay> at, TimeZone timeZone) {
        this.on = Collections.unmodifiableList(on);
        this.at = Collections.unmodifiableList(at);
        this.timeZone = timeZone;

        init();
    }

    @Override
    public ScheduleBuilder<WeeklyTrigger> getScheduleBuilder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List<CronTriggerImpl> buildCronTriggers() {
        List<CronTriggerImpl> result = new ArrayList<>();

        for (TimeOfDay timeOfDay : at) {
            CronTriggerImpl cronTigger = (CronTriggerImpl) CronScheduleBuilder.cronSchedule(createCronExpression(timeOfDay, on)).build();

            result.add(cronTigger);
        }

        return result;
    }

    public List<DayOfWeek> getOn() {
        return on;
    }

    public void setOn(List<DayOfWeek> on) {
        this.on = on;
    }

    public List<TimeOfDay> getAt() {
        return at;
    }

    public void setAt(List<TimeOfDay> at) {
        this.at = at;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (on.size() == 1) {
            builder.field("on", on.get(0).toString().toLowerCase());
        } else {
            builder.startArray("on");

            for (DayOfWeek dayOfWeek : on) {
                builder.value(dayOfWeek.toString().toLowerCase());
            }

            builder.endArray();
        }

        if (at.size() == 1) {
            builder.field("at", format(at.get(0)));
        } else {
            builder.startArray("at");

            for (TimeOfDay timeOfDay : at) {
                builder.value(format(timeOfDay));
            }

            builder.endArray();
        }

        builder.endObject();

        return builder;
    }

    public static WeeklyTrigger create(DocNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vDocNode = new ValidatingDocNode(jsonNode, validationErrors);

        List<DayOfWeek> on = vDocNode.get("on").asList().ofDayOfWeek();
        List<TimeOfDay> at = vDocNode.get("at").required().viaStringsAsList((s) -> parseTimeOfDay(s));

        validationErrors.throwExceptionForPresentErrors();

        return new WeeklyTrigger(on, at, timeZone);
    }

    private static int toQuartz(DayOfWeek dayOfWeek) {
        if (dayOfWeek == DayOfWeek.SUNDAY) {
            return DateBuilder.SUNDAY;
        } else {
            return dayOfWeek.getValue() + 1;
        }
    }

    private static CronExpression createCronExpression(TimeOfDay timeOfDay, List<DayOfWeek> on) {
        try {
            StringBuilder result = new StringBuilder();

            result.append(timeOfDay.getSecond()).append(' ');
            result.append(timeOfDay.getMinute()).append(' ');
            result.append(timeOfDay.getHour()).append(' ');
            result.append("? * ");

            if (on.size() == 0) {
                result.append("*");
            } else {
                boolean first = true;

                for (DayOfWeek dayOfWeek : on) {
                    if (first) {
                        first = false;
                    } else {
                        result.append(",");
                    }

                    result.append(toQuartz(dayOfWeek));
                }
            }

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static final TriggerFactory<WeeklyTrigger> FACTORY = new TriggerFactory<WeeklyTrigger>() {

        @Override
        public String getType() {
            return "weekly";
        }

        @Override
        public WeeklyTrigger create(DocNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
            return WeeklyTrigger.create(jsonNode, timeZone);
        }
    };
}
