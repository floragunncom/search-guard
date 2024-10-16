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

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.signals.settings.SignalsSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SignalsScheduleFactoryTest {

    private static final String CRON = "0 */2 * ? * *";
    private static final String INTERVAL = "10s";

    @Test
    public void testCronTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(emptySettings());

        Trigger trigger = signalsScheduleFactory.createCronTrigger(new JobKey("test-job"), CRON, null);
        int expectedInstruction = -1; //default setting value
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(cronMisfireSettings(-99));

        Trigger trigger = signalsScheduleFactory.createCronTrigger(new JobKey("test-job"), CRON, null);
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction(); //quartz default
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(cronMisfireSettings(-1));

        Trigger trigger = signalsScheduleFactory.createCronTrigger(new JobKey("test-job"), CRON, null);
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_fireAndProceedSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(cronMisfireSettings(1));

        Trigger trigger = signalsScheduleFactory.createCronTrigger(new JobKey("test-job"), CRON, null);
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_doNothingSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(cronMisfireSettings(2));

        Trigger trigger = signalsScheduleFactory.createCronTrigger(new JobKey("test-job"), CRON, null);
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(emptySettings());

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = -1; //default setting value
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(-99));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().build().getMisfireInstruction(); //quartz default
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(-1));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);;
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_fireNowSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(1));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nowWithExistingCountSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(2));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNowWithExistingCount().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nowWithRemainingCountSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(3));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNowWithRemainingCount().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nextWithRemainingCountSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(4));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNextWithRemainingCount().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nextWithExistingCountSettings() throws ConfigValidationException {
        SignalsScheduleFactory signalsScheduleFactory = new SignalsScheduleFactory(intervalMisfireSettings(5));

        Trigger trigger = signalsScheduleFactory.createIntervalScheduleTrigger(new JobKey("test-job"), INTERVAL);
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNextWithExistingCount().build().getMisfireInstruction();
        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    private SignalsSettings emptySettings() {
        return new SignalsSettings(Settings.EMPTY);
    }

    private SignalsSettings cronMisfireSettings(int misfire) {
        Settings settings = Settings.builder().put(SignalsSettings.SignalsStaticSettings.CRON_MISFIRE_STRATEGY.name(), misfire).build();
        return new SignalsSettings(settings);
    }

    private SignalsSettings intervalMisfireSettings(int misfire) {
        Settings settings = Settings.builder().put(SignalsSettings.SignalsStaticSettings.SIMPLE_MISFIRE_STRATEGY.name(), misfire).build();
        return new SignalsSettings(settings);
    }
}
