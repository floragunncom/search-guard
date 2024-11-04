/*
 * Copyright 2019-2021 floragunn GmbH
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

package com.floragunn.searchsupport.jobs.config.schedule;

import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.codec.digest.DigestUtils;
import org.quartz.*;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.schedule.elements.TriggerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.MutableTrigger;

public class DefaultScheduleFactory implements ScheduleFactory<ScheduleImpl> {
    public static DefaultScheduleFactory INSTANCE = new DefaultScheduleFactory(TriggerPostProcessor.NO_OP);

    protected String group = "main";
    private final TriggerPostProcessor triggerPostProcessor;

    public DefaultScheduleFactory(TriggerPostProcessor triggerPostProcessor) {
        this.triggerPostProcessor = triggerPostProcessor;
    }

    @Override
    public ScheduleImpl create(JobKey jobKey, DocNode objectNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(objectNode, validationErrors);
        List<Trigger> result = new ArrayList<Trigger>();

        if (vJsonNode.hasNonNull("schedule")) {
            try {
                result.addAll(createScheduleTriggers(jobKey, vJsonNode.get("schedule").asDocNode()));
            } catch (ConfigValidationException e) {
                validationErrors.add("schedule", e);
            }
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new ScheduleImpl(result);
    }

    protected List<Trigger> createScheduleTriggers(JobKey jobKey, DocNode jsonNode) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        ArrayList<Trigger> triggers = new ArrayList<>();

        ZoneId timeZoneId = vJsonNode.get("timezone").asTimeZoneId();
        TimeZone timeZone = timeZoneId != null ? TimeZone.getTimeZone(timeZoneId) : null;

        triggers.addAll(
                vJsonNode.get("cron").asList().withEmptyListAsDefault().ofObjectsParsedByString((s) -> createCronTrigger(jobKey, s, timeZone)));

        triggers.addAll(
                vJsonNode.get("interval").asList().withEmptyListAsDefault().ofObjectsParsedByString((s) -> createIntervalScheduleTrigger(jobKey, s)));

        for (TriggerFactory<?> scheduleFactory : TriggerFactory.FACTORIES) {
            triggers.addAll(vJsonNode.get(scheduleFactory.getType()).asList().withEmptyListAsDefault()
                    .ofObjectsParsedBy((n) -> createTrigger(jobKey, n, timeZone, scheduleFactory)));
        }

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return triggers;

    }

    protected String getTriggerKey(String trigger) {
        return DigestUtils.md5Hex(trigger);
    }

    protected Trigger createCronTrigger(JobKey jobKey, String cronExpression, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(cronExpression);

        try {
            MutableTrigger trigger = CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronExpression).inTimeZone(timeZone).build();
            trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));
            trigger.setJobKey(jobKey);
            trigger.setStartTime(new Date());
            triggerPostProcessor.processTrigger(trigger);

            return trigger;
        } catch (ParseException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, cronExpression,
                    "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15")
                            .message("Invalid cron expression: " + e.getMessage()).cause(e));
        }
    }

    protected Trigger createIntervalScheduleTrigger(JobKey jobKey, String interval) throws ConfigValidationException {
        String triggerKey = getTriggerKey(interval);
        Duration duration = DurationFormat.INSTANCE.parse(interval);

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(duration.toMillis()).build();
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));
        trigger.setJobKey(jobKey);
        trigger.setStartTime(new Date());
        triggerPostProcessor.processTrigger(trigger);

        return trigger;
    }

    protected Trigger createTrigger(JobKey jobKey, DocNode jsonNode, TimeZone timeZone, TriggerFactory<?> scheduleFactory)
            throws ConfigValidationException {
        String triggerKey = getTriggerKey(jsonNode.toString());

        MutableTrigger trigger = scheduleFactory.create(jsonNode, timeZone);
        trigger.setJobKey(jobKey);
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));
        triggerPostProcessor.processTrigger(trigger);

        return trigger;
    }

}
