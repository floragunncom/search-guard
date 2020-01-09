package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.validation.MissingAttribute;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;

public class MonthlyTrigger extends HumanReadableCronTrigger<MonthlyTrigger> {

    private static final long serialVersionUID = -6518785696829462600L;
    private List<Integer> on;
    private List<TimeOfDay> at;

    public MonthlyTrigger(List<Integer> on, List<TimeOfDay> at, TimeZone timeZone) {
        this.on = Collections.unmodifiableList(on);
        this.at = Collections.unmodifiableList(at);
        this.timeZone = timeZone;

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

    public static MonthlyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        List<Integer> on = null;
        List<TimeOfDay> at = null;

        JsonNode onNode = jsonNode.get("on");

        if (onNode != null && onNode.isArray()) {
            on = new ArrayList<>(onNode.size());

            for (JsonNode onNodeElement : onNode) {
                on.add(getMonth(onNodeElement, validationErrors));
            }
        } else if (onNode != null && onNode.isNumber()) {
            on = Collections.singletonList(getMonth(onNode, validationErrors));
        } else {
            on = Collections.emptyList();
        }

        try {
            JsonNode atNode = jsonNode.get("at");

            if (atNode != null && atNode.isArray()) {
                at = new ArrayList<>(atNode.size());

                for (JsonNode atNodeElement : atNode) {
                    at.add(parseTimeOfDay(atNodeElement.textValue()));
                }
            } else if (atNode != null && atNode.isTextual()) {
                at = Collections.singletonList(parseTimeOfDay(atNode.textValue()));
            } else {
                validationErrors.add(new MissingAttribute("at", jsonNode));
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("at", e);
        }

        return new MonthlyTrigger(on, at, timeZone);
    }

    private static int getMonth(JsonNode onNodeElement, ValidationErrors validationErrors) {
        if (onNodeElement.isNumber()) {
            int onValue = onNodeElement.asInt();

            if (onValue < 1 || onValue > 12) {
                validationErrors.add(new InvalidAttributeValue("on", onValue, "Month: 1-12", onNodeElement));
            }

            return onValue;
        } else {
            validationErrors.add(new InvalidAttributeValue("on", onNodeElement.toString(), "Month: 1-12", onNodeElement));

            return -1;
        }
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
        public MonthlyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
            return MonthlyTrigger.create(jsonNode, timeZone);
        }
    };

}
