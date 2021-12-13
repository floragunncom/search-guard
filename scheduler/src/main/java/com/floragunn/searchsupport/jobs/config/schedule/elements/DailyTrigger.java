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

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchsupport.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.config.validation.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidationErrors;

public class DailyTrigger extends HumanReadableCronTrigger<DailyTrigger> {

    private static final long serialVersionUID = 8163341554630381366L;

    private List<TimeOfDay> at;

    public DailyTrigger(List<TimeOfDay> at, TimeZone timeZone) {
        this.at = Collections.unmodifiableList(at);
        this.timeZone = timeZone;

        init();
    }

    @Override
    public ScheduleBuilder<DailyTrigger> getScheduleBuilder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List<CronTriggerImpl> buildCronTriggers() {
        List<CronTriggerImpl> result = new ArrayList<>();

        for (TimeOfDay timeOfDay : at) {
            CronTriggerImpl cronTigger = (CronTriggerImpl) CronScheduleBuilder.cronSchedule(createCronExpression(timeOfDay)).build();

            result.add(cronTigger);
        }

        return result;
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

    public static DailyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        List<TimeOfDay> at = null;

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

        return new DailyTrigger(at, timeZone);
    }

    public static final TriggerFactory<DailyTrigger> FACTORY = new TriggerFactory<DailyTrigger>() {

        @Override
        public String getType() {
            return "daily";
        }

        @Override
        public DailyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
            return DailyTrigger.create(jsonNode, timeZone);
        }
    };

    private static CronExpression createCronExpression(TimeOfDay timeOfDay) {
        try {
            StringBuilder result = new StringBuilder();

            result.append(timeOfDay.getSecond()).append(' ');
            result.append(timeOfDay.getMinute()).append(' ');
            result.append(timeOfDay.getHour()).append(' ');
            result.append("? * *");

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
