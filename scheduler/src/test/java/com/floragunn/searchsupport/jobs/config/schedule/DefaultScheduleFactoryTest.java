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

package com.floragunn.searchsupport.jobs.config.schedule;

import java.time.Duration;
import java.util.Collections;

import org.elasticsearch.common.Strings;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.JobKey;
import org.quartz.SimpleTrigger;
import org.quartz.TimeOfDay;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.searchsupport.jobs.config.schedule.elements.DailyTrigger;

public class DefaultScheduleFactoryTest {

    @Test
    public void daily() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"), DocNode.of("schedule.daily.at", "14:00:00"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        DailyTrigger dailyTrigger = (DailyTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), Collections.singletonList(TimeOfDay.hourMinuteAndSecondOfDay(14, 0, 0)), dailyTrigger.getAt());
        Assert.assertEquals(result.toString(), CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW, dailyTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"daily\":[{\"at\":\"14:00\"}]}", Strings.toString(result));
    }

    @Test
    public void daily_misfire() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"),
                DocNode.of("schedule.daily.at", "14:00:00", "schedule.when_late", "skip"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        DailyTrigger dailyTrigger = (DailyTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), Collections.singletonList(TimeOfDay.hourMinuteAndSecondOfDay(14, 0, 0)), dailyTrigger.getAt());
        Assert.assertEquals(result.toString(), CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING, dailyTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"when_late\":\"skip\",\"daily\":[{\"at\":\"14:00\"}]}", Strings.toString(result));
    }

    @Test
    public void cron() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"), DocNode.of("schedule.cron", "*/10 * * * * ?"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        CronTrigger cronTrigger = (CronTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), "*/10 * * * * ?", cronTrigger.getCronExpression());
        Assert.assertEquals(result.toString(), CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW, cronTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"timezone\":\"Europe/Berlin\",\"cron\":[\"*/10 * * * * ?\"]}", Strings.toString(result));
    }
    
    @Test
    public void cron_misfire() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"), DocNode.of("schedule.cron", "*/10 * * * * ?", "schedule.when_late", "skip"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        CronTrigger cronTrigger = (CronTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), "*/10 * * * * ?", cronTrigger.getCronExpression());
        Assert.assertEquals(result.toString(), CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING, cronTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"timezone\":\"Europe/Berlin\",\"when_late\":\"skip\",\"cron\":[\"*/10 * * * * ?\"]}", Strings.toString(result));
    }
    

    @Test
    public void interval() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"), DocNode.of("schedule.interval", "10m"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        SimpleTrigger simpleTrigger = (SimpleTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), Duration.ofMinutes(10).toMillis(), simpleTrigger.getRepeatInterval());
        Assert.assertEquals(result.toString(), SimpleTrigger.MISFIRE_INSTRUCTION_FIRE_NOW, simpleTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"interval\":[\"10m\"]}", Strings.toString(result));
    }
    
    @Test
    public void interval_misfire() throws Exception {
        ScheduleImpl result = DefaultScheduleFactory.INSTANCE.create(new JobKey("job"), DocNode.of("schedule.interval", "10m", "schedule.when_late", "skip"));

        Assert.assertEquals(result.toString(), 1, result.getTriggers().size());
        SimpleTrigger simpleTrigger = (SimpleTrigger) result.getTriggers().get(0);
        Assert.assertEquals(result.toString(), Duration.ofMinutes(10).toMillis(), simpleTrigger.getRepeatInterval());
        Assert.assertEquals(result.toString(), SimpleTrigger.MISFIRE_INSTRUCTION_RESCHEDULE_NEXT_WITH_EXISTING_COUNT, simpleTrigger.getMisfireInstruction());

        Assert.assertEquals("{\"when_late\":\"skip\",\"interval\":[\"10m\"]}", Strings.toString(result));
    }
}
