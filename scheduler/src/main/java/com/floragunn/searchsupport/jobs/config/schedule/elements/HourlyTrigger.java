package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.codova.validation.errors.MissingAttribute;
import com.floragunn.searchsupport.config.validation.ValidatingJsonNode;

public class HourlyTrigger extends HumanReadableCronTrigger<HourlyTrigger> {

    private static final long serialVersionUID = 8269041855326041719L;
    private List<Integer> minute;

    public HourlyTrigger(List<Integer> minute, TimeZone timeZone) {
        this.minute = Collections.unmodifiableList(minute);
        this.timeZone = timeZone;

        init();
    }

    @Override
    public ScheduleBuilder<HourlyTrigger> getScheduleBuilder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List<CronTriggerImpl> buildCronTriggers() {
        return Collections.singletonList((CronTriggerImpl) CronScheduleBuilder.cronSchedule(createCronExpression(this.minute)).build());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (minute.size() == 1) {
            builder.field("minute", minute.get(0));
        } else {
            builder.startArray("minute");

            for (Integer m : minute) {
                builder.value(m);
            }

            builder.endArray();
        }

        builder.endObject();

        return builder;
    }

    public static HourlyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingJsonNode vJsonNode = new ValidatingJsonNode(jsonNode, validationErrors);

        List<Integer> minute = null;

        try {
            JsonNode minuteNode = vJsonNode.get("minute");

            if (minuteNode != null && minuteNode.isArray()) {
                minute = new ArrayList<>(minuteNode.size());

                for (JsonNode minuteNodeElement : minuteNode) {
                    minute.add(parseMinute(minuteNodeElement));
                }
            } else if (minuteNode != null) {
                minute = Collections.singletonList(parseMinute(minuteNode));
            } else {
                validationErrors.add(new MissingAttribute("minute", jsonNode));
            }
        } catch (ConfigValidationException e) {
            validationErrors.add("minute", e);
        }

        vJsonNode.validateUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new HourlyTrigger(minute, timeZone);
    }

    private static int parseMinute(JsonNode minuteNode) throws ConfigValidationException {
        if (!minuteNode.isNumber()) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, minuteNode.textValue(), "number between 0 and 59"));
        }
        int result = minuteNode.asInt();

        if (result < 0 || result > 59) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, minuteNode.textValue(), "number between 0 and 59"));
        }

        return result;
    }

    private static CronExpression createCronExpression(List<Integer> minutes) {
        try {
            StringBuilder result = new StringBuilder();

            result.append("0").append(' ');
            result.append(Strings.join(minutes, ',')).append(' ');
            result.append("* ? * *");

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Integer> getMinute() {
        return minute;
    }

    public void setMinute(List<Integer> minute) {
        this.minute = minute;
    }

    public static final TriggerFactory<HourlyTrigger> FACTORY = new TriggerFactory<HourlyTrigger>() {

        @Override
        public String getType() {
            return "hourly";
        }

        @Override
        public HourlyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
            return HourlyTrigger.create(jsonNode, timeZone);
        }
    };

}
