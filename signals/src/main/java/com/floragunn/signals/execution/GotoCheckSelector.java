package com.floragunn.signals.execution;

import java.util.HashSet;
import java.util.Set;

import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.checks.Check;

public class GotoCheckSelector {
    private final Watch watch;
    private final String goTo;
    private final boolean skipAllChecks;
    private final Set<Check> skipChecks;

    public GotoCheckSelector(Watch watch, String goTo) {
        this.watch = watch;
        this.goTo = goTo;

        if (goTo == null) {
            this.skipAllChecks = false;
            this.skipChecks = null;
        } else if ("_actions".equalsIgnoreCase(goTo)) {
            this.skipAllChecks = true;
            this.skipChecks = null;
        } else {
            this.skipAllChecks = false;
            this.skipChecks = findChecksBefore(goTo);
        }

    }

    boolean isSelected(Check check) {
        if (this.goTo == null) {
            return true;
        } else if (skipAllChecks) {
            return false;
        } else if (skipChecks != null) {
            return !skipChecks.contains(check);
        } else {
            return true;
        }
    }

    private Set<Check> findChecksBefore(String selectedCheck) {
        Set<Check> result = new HashSet<>();

        for (Check check : this.watch.getChecks()) {
            if (selectedCheck.equalsIgnoreCase(check.getName())) {
                return result;
            } else {
                result.add(check);
            }
        }

        throw new IllegalArgumentException("No such check: " + selectedCheck);
    }

    @Override
    public String toString() {
        return "GotoCheckSelector [goTo=" + goTo + "]";
    }
}
