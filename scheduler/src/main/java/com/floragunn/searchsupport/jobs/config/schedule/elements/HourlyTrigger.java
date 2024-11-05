package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidatingDocNode;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory.MisfireStrategy;

public class HourlyTrigger extends HumanReadableCronTrigger<HourlyTrigger> {

    private static final long serialVersionUID = 8269041855326041719L;
    private List<Integer> minute;

    public HourlyTrigger(List<Integer> minute, TimeZone timeZone, MisfireStrategy misfireStrategy) {
        this.minute = Collections.unmodifiableList(minute);
        this.timeZone = timeZone;
        this.misfireStrategy = misfireStrategy;

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

    public static HourlyTrigger create(DocNode jsonNode, TimeZone timeZone, MisfireStrategy misfireStrategy) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();
        ValidatingDocNode vJsonNode = new ValidatingDocNode(jsonNode, validationErrors);

        List<Integer> minute = vJsonNode.get("minute").required().asList().inRange(0, 59).ofIntegers();

        vJsonNode.checkForUnusedAttributes();
        validationErrors.throwExceptionForPresentErrors();

        return new HourlyTrigger(minute, timeZone, misfireStrategy);
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
        public HourlyTrigger create(DocNode jsonNode, TimeZone timeZone, MisfireStrategy misfireStrategy) throws ConfigValidationException {
            return HourlyTrigger.create(jsonNode, timeZone, misfireStrategy);
        }
    };

}
