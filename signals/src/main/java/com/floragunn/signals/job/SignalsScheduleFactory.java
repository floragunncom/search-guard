/*
 * Copyright 2024 floragunn GmbH
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

package com.floragunn.signals.job;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory;
import com.floragunn.signals.settings.SignalsSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import java.text.ParseException;
import java.time.Duration;
import java.util.Objects;
import java.util.TimeZone;

public class SignalsScheduleFactory extends DefaultScheduleFactory {

    private static final Logger LOG = LogManager.getLogger(SignalsScheduleFactory.class);
    private final Integer cronMisfireStrategy;
    private final Integer simpleMisfireStrategy;

    public SignalsScheduleFactory(SignalsSettings signalsSettings) {
        this.cronMisfireStrategy = signalsSettings.getStaticSettings().getCronMisfireStrategy();
        this.simpleMisfireStrategy = signalsSettings.getStaticSettings().getSimpleMisfireStrategy();
    }

    @Override
    protected Trigger createCronTrigger(JobKey jobKey, String cronExpression, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(cronExpression);

        try {

            CronScheduleBuilder schedule = CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronExpression).inTimeZone(timeZone);

            if(Objects.nonNull(cronMisfireStrategy)) {

                switch (cronMisfireStrategy) {
                    case 1: schedule.withMisfireHandlingInstructionFireAndProceed(); break;
                    case 2: schedule.withMisfireHandlingInstructionDoNothing(); break;
                    case -1: schedule.withMisfireHandlingInstructionIgnoreMisfires(); break;
                    default: LOG.error("Invalid cron misfire strategy: {}", cronMisfireStrategy);
                }

                LOG.debug("Cron misfire strategy: {}", cronMisfireStrategy);
            } else {
                LOG.debug("Using quartz's default cron misfire strategy");
            }

            return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                    .withSchedule(schedule).build();
        } catch (ParseException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, cronExpression,
                    "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15")
                    .message("Invalid cron expression: " + e.getMessage()).cause(e));
        }
    }

    @Override
    protected Trigger createIntervalScheduleTrigger(JobKey jobKey, String interval) throws ConfigValidationException {
        String triggerKey = getTriggerKey(interval);
        Duration duration = DurationFormat.INSTANCE.parse(interval);
        SimpleScheduleBuilder schedule = SimpleScheduleBuilder.simpleSchedule()
                .repeatForever().withIntervalInMilliseconds(duration.toMillis());

        if(Objects.nonNull(simpleMisfireStrategy)) {

            switch (simpleMisfireStrategy) {
                case 1: schedule.withMisfireHandlingInstructionFireNow();break;
                case -1: schedule.withMisfireHandlingInstructionIgnoreMisfires();break;
                case 5: schedule.withMisfireHandlingInstructionNextWithExistingCount();break;
                case 4: schedule.withMisfireHandlingInstructionNextWithRemainingCount();break;
                case 2: schedule.withMisfireHandlingInstructionNowWithExistingCount();break;
                case 3: schedule.withMisfireHandlingInstructionNowWithRemainingCount();break;
                default: LOG.error("Invalid simple misfire strategy: {}", simpleMisfireStrategy);
            }

            LOG.debug("Simple misfire strategy: {}", simpleMisfireStrategy);
        } else {
            LOG.debug("Using quartz's default simple misfire strategy");
        }


        return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                .withSchedule(schedule).build();
    }

}
