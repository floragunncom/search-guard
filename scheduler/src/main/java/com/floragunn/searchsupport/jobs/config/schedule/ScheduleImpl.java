package com.floragunn.searchsupport.jobs.config.schedule;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.quartz.CronTrigger;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;

import com.floragunn.codova.config.temporal.DurationFormat;
import com.floragunn.searchsupport.jobs.config.schedule.elements.DailyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.HumanReadableCronTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.MonthlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.elements.WeeklyTrigger;

public class ScheduleImpl implements Schedule {

    private List<Trigger> triggers;

    public ScheduleImpl(List<Trigger> triggers) {
        this.triggers = Collections.unmodifiableList(triggers);
    }

    @Override
    public List<Trigger> getTriggers() {
        return triggers;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();

        TimeZone timeZone = this.getTimeZone();

        if (timeZone != null) {
            builder.field("timezone", timeZone.getID());
        }

        List<CronTrigger> cronTriggers = getCronTriggers();

        if (cronTriggers.size() > 0) {
            builder.field("cron").startArray();

            for (CronTrigger trigger : cronTriggers) {
                builder.value(trigger.getCronExpression());
            }

            builder.endArray();
        }

        List<SimpleTrigger> simpleTriggers = getIntervalTriggers();

        if (simpleTriggers.size() > 0) {
            builder.field("interval").startArray();

            for (SimpleTrigger trigger : simpleTriggers) {
                builder.value(DurationFormat.INSTANCE.format(Duration.ofMillis(trigger.getRepeatInterval())));
            }

            builder.endArray();
        }

        List<DailyTrigger> dailyTriggers = getTriggers(DailyTrigger.class);

        if (dailyTriggers.size() > 0) {
            builder.field("daily").startArray();

            for (DailyTrigger trigger : dailyTriggers) {
                trigger.toXContent(builder, params);
            }

            builder.endArray();
        }

        List<WeeklyTrigger> weeklyTriggers = getTriggers(WeeklyTrigger.class);

        if (weeklyTriggers.size() > 0) {
            builder.field("weekly").startArray();

            for (WeeklyTrigger trigger : weeklyTriggers) {
                trigger.toXContent(builder, params);
            }

            builder.endArray();
        }

        List<MonthlyTrigger> monthlyTriggers = getTriggers(MonthlyTrigger.class);

        if (monthlyTriggers.size() > 0) {
            builder.field("monthly").startArray();

            for (MonthlyTrigger trigger : monthlyTriggers) {
                trigger.toXContent(builder, params);
            }

            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    private TimeZone getTimeZone() {
        // XXX this is kinda hacky

        if (this.triggers == null) {
            return null;
        }

        for (Trigger trigger : this.triggers) {
            if (trigger instanceof CronTrigger) {
                return ((CronTrigger) trigger).getTimeZone();
            } else if (trigger instanceof HumanReadableCronTrigger) {
                return ((HumanReadableCronTrigger<?>) trigger).getTimeZone();
            }
        }

        return null;
    }

    private List<CronTrigger> getCronTriggers() {
        ArrayList<CronTrigger> result = new ArrayList<>(this.triggers.size());

        for (Trigger trigger : triggers) {
            if (trigger instanceof CronTrigger) {
                result.add((CronTrigger) trigger);
            }
        }

        return result;
    }

    private List<SimpleTrigger> getIntervalTriggers() {
        ArrayList<SimpleTrigger> result = new ArrayList<>(this.triggers.size());

        for (Trigger trigger : triggers) {
            if (trigger instanceof SimpleTrigger) {
                result.add((SimpleTrigger) trigger);
            }
        }

        return result;
    }

    private <X extends Trigger> List<X> getTriggers(Class<X> type) {
        ArrayList<X> result = new ArrayList<>(this.triggers.size());

        for (Trigger trigger : triggers) {
            if (type.isAssignableFrom(trigger.getClass())) {
                @SuppressWarnings("unchecked")
                X typedTrigger = (X) trigger;
                result.add(typedTrigger);
            }
        }

        return result;
    }

}
