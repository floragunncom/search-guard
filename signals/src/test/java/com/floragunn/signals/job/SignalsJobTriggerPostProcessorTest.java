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

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.schedule.elements.DailyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.HourlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.MonthlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.WeeklyTrigger;
import com.floragunn.signals.settings.SignalsSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.CronScheduleBuilder;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.MutableTrigger;

import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SignalsJobTriggerPostProcessorTest {

    private static final String CRON = "0 */2 * ? * *";

    @Test
    public void testCronTriggerMisfireStrategy_emptySettings()  {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = CronScheduleBuilder.cronSchedule(CRON).build();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_unsupportedMisfireSettingValue() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-99));

        MutableTrigger trigger = CronScheduleBuilder.cronSchedule(CRON).build();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_ignoreMisfireSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-1));

        MutableTrigger trigger = CronScheduleBuilder.cronSchedule(CRON).build();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_fireAndProceedSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(1));

        MutableTrigger trigger = CronScheduleBuilder.cronSchedule(CRON).build();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_doNothingSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(2));

        MutableTrigger trigger = CronScheduleBuilder.cronSchedule(CRON).build();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_emptySettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().build().getMisfireInstruction(); //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_unsupportedMisfireSettingValue() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(-99));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().build().getMisfireInstruction(); //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_ignoreMisfireSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(-1));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_fireNowSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(1));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nowWithExistingCountSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(2));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNowWithExistingCount().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nowWithRemainingCountSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(3));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNowWithRemainingCount().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nextWithRemainingCountSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(4));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNextWithRemainingCount().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_nextWithExistingCountSettings() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(5));

        MutableTrigger trigger = SimpleScheduleBuilder.simpleSchedule().build();
        int expectedInstruction = SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionNextWithExistingCount().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeHourlyTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = hourlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeHourlyTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-99));

        MutableTrigger trigger = hourlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeHourlyTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-1));

        MutableTrigger trigger = hourlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeHourlyTriggerMisfireStrategy_fireAndProceedSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(1));

        MutableTrigger trigger = hourlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeHourlyTriggerMisfireStrategy_doNothingSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(2));

        MutableTrigger trigger = hourlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeDailyTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = dailyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeDailyTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-99));

        MutableTrigger trigger = dailyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeDailyTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-1));

        MutableTrigger trigger = dailyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeDailyTriggerMisfireStrategy_fireAndProceedSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(1));

        MutableTrigger trigger = dailyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeDailyTriggerMisfireStrategy_doNothingSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(2));

        MutableTrigger trigger = dailyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeWeeklyTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = weeklyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeWeeklyTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-99));

        MutableTrigger trigger = weeklyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeWeeklyTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-1));

        MutableTrigger trigger = weeklyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeWeeklyTriggerMisfireStrategy_fireAndProceedSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(1));

        MutableTrigger trigger = weeklyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeWeeklyTriggerMisfireStrategy_doNothingSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(2));

        MutableTrigger trigger = weeklyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeMonthlyTriggerMisfireStrategy_emptySettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(emptySettings());

        MutableTrigger trigger = monthlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeMonthlyTriggerMisfireStrategy_unsupportedMisfireSettingValue() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-99));

        MutableTrigger trigger = monthlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).build().getMisfireInstruction();; //quartz default

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeMonthlyTriggerMisfireStrategy_ignoreMisfireSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(-1));

        MutableTrigger trigger = monthlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionIgnoreMisfires().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeMonthlyTriggerMisfireStrategy_fireAndProceedSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(1));

        MutableTrigger trigger = monthlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionFireAndProceed().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testDateAndTimeMonthlyTriggerMisfireStrategy_doNothingSettings() throws ConfigValidationException {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(2));

        MutableTrigger trigger = monthlyTrigger();
        int expectedInstruction = CronScheduleBuilder.cronSchedule(CRON).withMisfireHandlingInstructionDoNothing().build().getMisfireInstruction();

        triggerPostProcessor.processTrigger(trigger);

        assertThat(trigger.getMisfireInstruction(), equalTo(expectedInstruction));
    }

    @Test
    public void testCronTriggerMisfireStrategy_shouldNotModifyTriggerWhenMisfireStrategyIsNotConfigured() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(cronMisfireSettings(0));
        CronTriggerImpl cronTrigger = Mockito.mock(CronTriggerImpl.class);

        triggerPostProcessor.processTrigger(cronTrigger);

        Mockito.verifyNoInteractions(cronTrigger);
    }

    @Test
    public void testIntervalTriggerMisfireStrategy_shouldNotModifyTriggerWhenMisfireStrategyIsNotConfigured() {
        SignalsJobTriggerPostProcessor triggerPostProcessor = new SignalsJobTriggerPostProcessor(intervalMisfireSettings(0));
        SimpleTriggerImpl simpleTrigger = Mockito.mock(SimpleTriggerImpl.class);

        triggerPostProcessor.processTrigger(simpleTrigger);

        Mockito.verifyNoInteractions(simpleTrigger);

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

    private HourlyTrigger hourlyTrigger() throws ConfigValidationException {
        return HourlyTrigger.FACTORY.create(DocNode.of("minute", 10), TimeZone.getDefault());
    }

    private DailyTrigger dailyTrigger() throws ConfigValidationException {
        return DailyTrigger.FACTORY.create(DocNode.of("at", "10:00:00"), TimeZone.getDefault());
    }

    private WeeklyTrigger weeklyTrigger() throws ConfigValidationException {
        return WeeklyTrigger.FACTORY.create(DocNode.of("on", "monday", "at", "10:00:00"), TimeZone.getDefault());
    }

    private MonthlyTrigger monthlyTrigger() throws ConfigValidationException {
        return MonthlyTrigger.FACTORY.create(DocNode.of("on", "1", "at", "10:00:00"), TimeZone.getDefault());
    }
}
