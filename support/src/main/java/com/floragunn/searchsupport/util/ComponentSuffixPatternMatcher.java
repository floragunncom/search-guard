package com.floragunn.searchsupport.util;

import com.floragunn.codova.config.text.Pattern;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.fluent.collections.ImmutableList;
import com.floragunn.searchsupport.meta.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ComponentSuffixPatternMatcher {

    private final static Logger log = LogManager.getLogger(ComponentSuffixPatternMatcher.class);

    private final Pattern pattern;

    private final String suffix;

    private final int suffixLength;

    private final Component component;

    public static ComponentSuffixPatternMatcher create(List<String> patternStrings) throws ConfigValidationException {
        if (patternStrings.isEmpty()) {
            return new ComponentSuffixPatternMatcher(Pattern.create(patternStrings), Component.NONE);
        }
        Component component = Component.extractComponent(patternStrings.get(0));
        for (String patternString : patternStrings) {
            Component currentComponent = Component.extractComponent(patternString);
            if (component != currentComponent) {
                return mixedComponentsPattern(component, currentComponent);
            }
        }
        int currentSuffixLength = component.getComponentSuffixWithSeparator().length();
        List<@NonNull String> patternsWithoutSuffixes = patternStrings.stream() //
                .map(pattern -> removeSuffix(pattern, currentSuffixLength)) //
                .collect(ImmutableList.collector());
        return new ComponentSuffixPatternMatcher(Pattern.create(patternsWithoutSuffixes), component);
    }

    private static @NonNull ComponentSuffixPatternMatcher mixedComponentsPattern(Component component, Component currentComponent) {
        // TODO consider throwing exception here

        //               throw new ConfigValidationException(new ValidationError("patterns", "Index patterns contains various components"));
        log.error("Index pattern is related to various components e.g. '{}' and '{}'", component, currentComponent);
        Pattern blank = Pattern.join(Collections.emptyList());
        return new ComponentSuffixPatternMatcher(blank, Component.NONE);
    }

    static ComponentSuffixPatternMatcher create(String patternString) throws ConfigValidationException {
        Component component = Component.extractComponent(patternString);
        int currentSuffixLength = component.getComponentSuffixWithSeparator().length();
        Pattern codovaPattern = Pattern.create(removeSuffix(patternString, currentSuffixLength));
        return new ComponentSuffixPatternMatcher(codovaPattern, component);
    }

    public static ComponentSuffixPatternMatcher join(List<ComponentSuffixPatternMatcher> componentSuffixPatternMatchers) {
        // TODO unit test needed for the class
        if (componentSuffixPatternMatchers.isEmpty()) {
            return new ComponentSuffixPatternMatcher(Pattern.join(Collections.emptyList()), Component.NONE);
        }
        Component component = componentSuffixPatternMatchers.get(0).component;
        List<Pattern> codovaPatterns = new ArrayList<>(componentSuffixPatternMatchers.size());
        for (ComponentSuffixPatternMatcher componentSuffixPatternMatcher : componentSuffixPatternMatchers) {
            if (component != componentSuffixPatternMatcher.component) {
                return mixedComponentsPattern(component, componentSuffixPatternMatcher.component);
            }
            codovaPatterns.add(componentSuffixPatternMatcher.pattern);
        }
        return  new ComponentSuffixPatternMatcher(Pattern.join(codovaPatterns), component);
    }

    ComponentSuffixPatternMatcher(Pattern pattern, Component component) {
        this.pattern = Objects.requireNonNull(pattern, "Pattern must not be null");
        this.component = Objects.requireNonNull(component, "Component must not be null");
        this.suffix = component.getComponentSuffixWithSeparator();
        this.suffixLength = this.suffix.length();
    }

    public boolean matches(String string) {
        if (suffixLength == 0) {
            if (string.contains(Component.COMPONENT_SEPARATOR)) {
                return false;
            } else {
                return pattern.matches(string);
            }
        } else if(string.endsWith(suffix)) {
            String stringWithoutSuffix = removeSuffix(string, suffixLength);
            return (!stringWithoutSuffix.isBlank()) && pattern.matches(stringWithoutSuffix);
        }
        return false;
    }

    private static @NonNull String removeSuffix(String string, int length) {
        return string.substring(0, string.length() - length);
    }

    public boolean isBlank() {
        return pattern.isBlank();
    }

    public boolean isWildcard() {
        return (suffixLength == 0) && pattern.isWildcard();
    }
}
