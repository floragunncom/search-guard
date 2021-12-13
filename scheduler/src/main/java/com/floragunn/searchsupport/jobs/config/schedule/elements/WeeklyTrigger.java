package com.floragunn.searchsupport.jobs.config.schedule.elements;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

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

    public static WeeklyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        List<DayOfWeek> on = null;
        List<TimeOfDay> at = null;

        try {
            JsonNode onNode = jsonNode.get("on");

            if (onNode != null && onNode.isArray()) {
                on = new ArrayList<>(onNode.size());

                for (JsonNode onNodeElement : onNode) {
                    on.add(getDayOfWeek(onNodeElement.textValue()));
                }
            } else if (onNode != null && onNode.isTextual()) {
                on = Collections.singletonList(getDayOfWeek(onNode.textValue()));
            } else {
                on = Collections.emptyList();
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("on", e);
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

    private static DayOfWeek getDayOfWeek(String string) throws ConfigValidationException {
        switch (string) {
        case "sunday":
        case "sun":
            return DayOfWeek.SUNDAY;
        case "monday":
        case "mon":
            return DayOfWeek.MONDAY;
        case "tuesday":
        case "tue":
            return DayOfWeek.TUESDAY;
        case "wednesday":
        case "wed":
            return DayOfWeek.WEDNESDAY;
        case "thursday":
        case "thu":
            return DayOfWeek.THURSDAY;
        case "friday":
        case "fri":
            return DayOfWeek.FRIDAY;
        case "saturday":
        case "sat":
            return DayOfWeek.SATURDAY;
        default:
            throw new ConfigValidationException(new InvalidAttributeValue("on", string, "mon|tue|wed|thu|fri|sat|sun"));
        }
    }

    public static final TriggerFactory<WeeklyTrigger> FACTORY = new TriggerFactory<WeeklyTrigger>() {

        @Override
        public String getType() {
            return "weekly";
        }

        @Override
        public WeeklyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
            return WeeklyTrigger.create(jsonNode, timeZone);
        }
    };
}
