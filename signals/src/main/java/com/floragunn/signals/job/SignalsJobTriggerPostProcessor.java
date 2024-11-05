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

import com.floragunn.searchsupport.jobs.config.schedule.TriggerPostProcessor;
import com.floragunn.searchsupport.jobs.config.schedule.elements.DailyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.HourlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.MonthlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.WeeklyTrigger;
import com.floragunn.signals.settings.SignalsSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronTrigger;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.MutableTrigger;

import static com.floragunn.signals.settings.SignalsSettings.SignalsStaticSettings.SCHEDULER_DEFAULT_MISFIRE_STRATEGY_VALUE;

public class SignalsJobTriggerPostProcessor implements TriggerPostProcessor {

    private static final Logger LOG = LogManager.getLogger(SignalsJobTriggerPostProcessor.class);
    private final int cronMisfireStrategy;
    private final int simpleMisfireStrategy;

    public SignalsJobTriggerPostProcessor(SignalsSettings signalsSettings) {
        this.cronMisfireStrategy = signalsSettings.getStaticSettings().getCronMisfireStrategy();
        this.simpleMisfireStrategy = signalsSettings.getStaticSettings().getSimpleMisfireStrategy();
    }

    @Override
    public void processTrigger(MutableTrigger trigger) {
        if (trigger instanceof CronTriggerImpl cronTrigger) {
            processCronTrigger(cronTrigger);
        } else if (trigger instanceof SimpleTriggerImpl simpleTrigger) {
            processSimpleTrigger(simpleTrigger);
        } else if (trigger instanceof DailyTrigger dailyTrigger) { //todo is another config variable needed for hourly, daily, monthly and weekly triggers?
            processCronTrigger(dailyTrigger);
        } else if (trigger instanceof HourlyTrigger hourlyTrigger) {
            processCronTrigger(hourlyTrigger);
        } else if (trigger instanceof MonthlyTrigger monthlyTrigger) {
            processCronTrigger(monthlyTrigger);
        } else if (trigger instanceof WeeklyTrigger weeklyTrigger) {
            processCronTrigger(weeklyTrigger);
        } else {
            throw new IllegalArgumentException(String.format("Trigger of type: %s is not supported", trigger.getClass().getSimpleName()));
        }
    }

    private void processCronTrigger(MutableTrigger cronTrigger) {
        if(SCHEDULER_DEFAULT_MISFIRE_STRATEGY_VALUE != cronMisfireStrategy) {

            switch (cronMisfireStrategy) {
                case 1: cronTrigger.setMisfireInstruction(CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW); break;
                case 2: cronTrigger.setMisfireInstruction(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING); break;
                case -1: cronTrigger.setMisfireInstruction(Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY); break;
                default: LOG.error("Invalid cron misfire strategy: {}", cronMisfireStrategy);
            }

            LOG.debug("Cron misfire strategy: {}", cronMisfireStrategy);
        } else {
            LOG.debug("Using quartz's default cron misfire strategy");
        }
    }

    private void processSimpleTrigger(MutableTrigger trigger) {
        if(SCHEDULER_DEFAULT_MISFIRE_STRATEGY_VALUE != simpleMisfireStrategy) {

            switch (simpleMisfireStrategy) {
                case 1: trigger.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW); break;
                case -1: trigger.setMisfireInstruction(Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY); break;
                case 5: trigger.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_EXISTING_COUNT); break;
                case 4: trigger.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_REMAINING_COUNT); break;
                case 2: trigger.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_EXISTING_REPEAT_COUNT); break;
                case 3: trigger.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NOW_WITH_REMAINING_REPEAT_COUNT); break;
                default: LOG.error("Invalid simple misfire strategy: {}", simpleMisfireStrategy);
            }

            LOG.debug("Simple misfire strategy: {}", simpleMisfireStrategy);
        } else {
            LOG.debug("Using quartz's default simple misfire strategy");
        }
    }
}
