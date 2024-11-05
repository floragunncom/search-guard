package com.floragunn.searchsupport.jobs.config.schedule.elements;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.xcontent.ToXContentObject;
import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.TimeOfDay;
import org.quartz.Trigger;
import org.quartz.impl.triggers.AbstractTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.ValidationErrors;
import com.floragunn.codova.validation.errors.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.schedule.DefaultScheduleFactory.MisfireStrategy;

public abstract class HumanReadableCronTrigger<T extends Trigger> extends AbstractTrigger<T> implements Trigger, ToXContentObject {

    private static final long serialVersionUID = 8008213171103409490L;

    private List<CronTriggerImpl> generatedCronTriggers;
    private Date startTime = null;
    private Date endTime = null;
    private Date previousFireTime = null;
    protected TimeZone timeZone;
    protected MisfireStrategy misfireStrategy;

    protected abstract List<CronTriggerImpl> buildCronTriggers();

    protected void init() {
        this.generatedCronTriggers = buildCronTriggers();

        if (timeZone != null) {
            for (CronTriggerImpl trigger : this.generatedCronTriggers) {
                trigger.setTimeZone(timeZone);
            }
        }
        
        setMisfireInstruction(misfireStrategy == MisfireStrategy.SKIP ? CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING : CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW);

        for (CronTriggerImpl trigger : this.generatedCronTriggers) {
            trigger.setMisfireInstruction(getMisfireInstruction());
        }
    }
    
    @Override
    public void setNextFireTime(Date nextFireTime) {
        CronTriggerImpl trigger = getNextFireTimeCronTrigger();

        if (trigger != null) {
            trigger.setNextFireTime(nextFireTime);
        } else {

            // We're being recovered, so compute the fire times
            // XXX We don't have a calendar object here

            Date justBeforeNextFireTime = new Date(nextFireTime.getTime() - 1);

            for (CronTriggerImpl delegate : generatedCronTriggers) {
                Date fireTime = delegate.getFireTimeAfter(justBeforeNextFireTime);

                delegate.setNextFireTime(fireTime);
            }
        }
    }

    @Override
    public void setPreviousFireTime(Date previousFireTime) {
        this.previousFireTime = previousFireTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setPreviousFireTime(previousFireTime);
        }
    }

    @Override
    public void triggered(Calendar calendar) {
        Date oldNextFireTime = getNextFireTime();
        previousFireTime = oldNextFireTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            if (delegate.getNextFireTime() == null || !delegate.getNextFireTime().after(oldNextFireTime)) {
                delegate.setNextFireTime(getFireTimeAfter(delegate, oldNextFireTime, calendar));

                delegate.setPreviousFireTime(previousFireTime);
            }
        }
    }

    @Override
    public Date computeFirstFireTime(Calendar calendar) {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFirstFireTime = delegate.computeFirstFireTime(calendar);

            if (delegateFirstFireTime != null && (result == null || result.after(delegateFirstFireTime))) {
                result = delegateFirstFireTime;
            }
        }

        return result;
    }

    @Override
    public boolean mayFireAgain() {
        boolean result = false;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            result |= delegate.mayFireAgain();
        }

        return result;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public void setStartTime(Date startTime) {
        this.startTime = startTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setStartTime(startTime);
        }
    }

    @Override
    public void setEndTime(Date endTime) {
        this.endTime = endTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setEndTime(endTime);
        }
    }

    @Override
    public Date getEndTime() {
        return endTime;
    }

    @Override
    public Date getNextFireTime() {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateNextFireTime = delegate.getNextFireTime();

            if (delegateNextFireTime != null && (result == null || result.after(delegateNextFireTime))) {
                result = delegateNextFireTime;
            }
        }

        return result;
    }

    @Override
    public Date getPreviousFireTime() {
        return previousFireTime;
    }

    @Override
    public Date getFireTimeAfter(Date afterTime) {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFireTimeAfter = delegate.getFireTimeAfter(afterTime);

            if (delegateFireTimeAfter != null && (result == null || result.after(delegateFireTimeAfter))) {
                result = delegateFireTimeAfter;
            }
        }

        return result;
    }

    @Override
    public Date getFinalFireTime() {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFinalFireTime = delegate.getFinalFireTime();

            if (delegateFinalFireTime != null && (result == null || result.before(delegateFinalFireTime))) {
                result = delegateFinalFireTime;
            }
        }

        return result;
    }

    @Override
    protected boolean validateMisfireInstruction(int candidateMisfireInstruction) {
        return candidateMisfireInstruction >= MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY
                && candidateMisfireInstruction <= CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING;
    }

    @Override
    public void updateAfterMisfire(Calendar cal) {
        int misfireInstruction = getMisfireInstruction();

        if (misfireInstruction == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return;
        }

        if (misfireInstruction == MISFIRE_INSTRUCTION_SMART_POLICY) {
            misfireInstruction = CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
        }

        if (misfireInstruction == CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING) {
            Date now = new Date();

            for (CronTriggerImpl delegate : generatedCronTriggers) {
                delegate.setNextFireTime(getFireTimeAfter(delegate, now, cal));
            }
        } else if (misfireInstruction == CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW) {

            Date now = new Date();
            CronTriggerImpl earliest = null;

            for (CronTriggerImpl delegate : generatedCronTriggers) {
                delegate.setNextFireTime(getFireTimeAfter(delegate, now, cal));

                if (earliest == null || earliest.getNextFireTime().after(delegate.getNextFireTime())) {
                    earliest = delegate;
                }
            }

            earliest.setNextFireTime(new Date());
        }
    }

    @Override
    public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.updateWithNewCalendar(cal, misfireThreshold);
        }
    }

    private Date getFireTimeAfter(CronTriggerImpl trigger, Date afterTime, Calendar cal) {
        Date result = trigger.getFireTimeAfter(afterTime);
        while (result != null && cal != null && !cal.isTimeIncluded(result.getTime())) {
            result = trigger.getFireTimeAfter(result);
        }

        return result;
    }

    private CronTriggerImpl getNextFireTimeCronTrigger() {
        CronTriggerImpl result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateNextFireTime = delegate.getNextFireTime();

            if (delegateNextFireTime != null
                    && (result == null || result.getNextFireTime() == null || result.getNextFireTime().after(delegateNextFireTime))) {
                result = delegate;
            }
        }

        return result;
    }

    protected static String format(TimeOfDay timeOfDay) {
        StringBuilder result = new StringBuilder();

        result.append(timeOfDay.getHour());
        result.append(':');

        if (timeOfDay.getMinute() < 10) {
            result.append('0');
        }

        result.append(timeOfDay.getMinute());

        if (timeOfDay.getSecond() != 0) {
            result.append(':');

            if (timeOfDay.getSecond() < 10) {
                result.append('0');
            }

            result.append(timeOfDay.getSecond());
        }

        return result.toString();
    }

    protected static TimeOfDay parseTimeOfDay(String string) throws ConfigValidationException {
        final String expected = "Time of day: <HH>:<MM>:<SS>?";

        try {

            int colon = string.indexOf(':');

            if (colon == -1) {
                int hour = Integer.parseInt(string);

                if (hour < 0 || hour >= 24) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, expected).message("Hour must be between 0 and 23"));
                }

                return new TimeOfDay(hour, 0);
            } else {
                int hour = Integer.parseInt(string.substring(0, colon));
                int minute;
                int second = 0;

                int nextColon = string.indexOf(':', colon + 1);

                if (nextColon == -1) {
                    minute = Integer.parseInt(string.substring(colon + 1));
                } else {
                    minute = Integer.parseInt(string.substring(colon + 1, nextColon));
                    second = Integer.parseInt(string.substring(nextColon + 1));
                }

                ValidationErrors validationErrors = new ValidationErrors();

                if (hour < 0 || hour >= 24) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, expected).message("Hour must be between 0 and 23"));
                }

                if (minute < 0 || minute >= 60) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, expected).message("Minute must be between 0 and 59"));
                }

                if (second < 0 || second >= 60) {
                    throw new ConfigValidationException(new InvalidAttributeValue(null, string, expected).message("Second must be between 0 and 59"));
                }

                validationErrors.throwExceptionForPresentErrors();

                return new TimeOfDay(hour, minute, second);
            }

        } catch (NumberFormatException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, string, expected).cause(e));
        }
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }
}
