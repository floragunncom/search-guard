package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory.MisfireStrategy;

public class MonthlyTrigger extends HumanReadableCronTrigger<MonthlyTrigger> {

    private static final long serialVersionUID = -6518785696829462600L;
    private List<Integer> on;
    private List<TimeOfDay> at;

    public MonthlyTrigger(List<Integer> on, List<TimeOfDay> at, TimeZone timeZone, MisfireStrategy misfireStrategy) {
        this.on = Collections.unmodifiableList(on);
        this.at = Collections.unmodifiableList(at);
        this.timeZone = timeZone;
        this.misfireStrategy = misfireStrategy;

        init();
    }

    @Override
    public ScheduleBuilder<MonthlyTrigger> getScheduleBuilder() {
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

    public List<Integer> getOn() {
        return on;
    }

    public void setOn(List<Integer> on) {
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
            builder.field("on", on.get(0));
        } else {
            builder.array("on", on);
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

    public static MonthlyTrigger create(DocNode jsonNode, TimeZone timeZone, MisfireStrategy misfireStrategy) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vDocNode = new ValidatingDocNode(jsonNode, validationErrors);

        List<Integer> on = vDocNode.get("on").expected("Day of Month: 1-31").asList().inRange(1, 31).ofIntegers();
        List<TimeOfDay> at = vDocNode.get("at").required().viaStringsAsList((s) -> parseTimeOfDay(s));

        if (on == null) {
            on = Collections.emptyList();
        }

        return new MonthlyTrigger(on, at, timeZone, misfireStrategy);
    }

    private static CronExpression createCronExpression(TimeOfDay timeOfDay, List<Integer> on) {
        try {
            StringBuilder result = new StringBuilder();

            result.append(timeOfDay.getSecond()).append(' ');
            result.append(timeOfDay.getMinute()).append(' ');
            result.append(timeOfDay.getHour()).append(' ');

            if (on.size() == 0) {
                result.append("*");
            } else {
                boolean first = true;

                for (Integer dayOfMonth : on) {
                    if (first) {
                        first = false;
                    } else {
                        result.append(",");
                    }

                    result.append(dayOfMonth);
                }
            }

            result.append(" * ?");

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static final TriggerFactory<MonthlyTrigger> FACTORY = new TriggerFactory<MonthlyTrigger>() {

        @Override
        public String getType() {
            return "monthly";
        }

        @Override
        public MonthlyTrigger create(DocNode jsonNode, TimeZone timeZone, MisfireStrategy misfireStrategy) throws ConfigValidationException {
            return MonthlyTrigger.create(jsonNode, timeZone, misfireStrategy);
        }
    };

}
