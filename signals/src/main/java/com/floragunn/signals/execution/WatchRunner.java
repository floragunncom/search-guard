/*
 * Copyright 2019-2022 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.floragunn.signals.execution;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.floragunn.signals.truststore.service.TrustManagerRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.floragunn.codova.config.temporal.DurationExpression;
import com.floragunn.searchsupport.diag.DiagnosticContext;
import com.floragunn.signals.accounts.AccountRegistry;
import com.floragunn.signals.script.types.SignalsObjectFunctionScript;
import com.floragunn.signals.settings.SignalsSettings;
import com.floragunn.signals.support.NestedValueMap;
import com.floragunn.signals.watch.Watch;
import com.floragunn.signals.watch.action.handlers.ActionExecutionResult;
import com.floragunn.signals.watch.action.invokers.ActionInvocationType;
import com.floragunn.signals.watch.action.invokers.ActionInvoker;
import com.floragunn.signals.watch.action.invokers.AlertAction;
import com.floragunn.signals.watch.action.invokers.ResolveAction;
import com.floragunn.signals.watch.checks.Check;
import com.floragunn.signals.watch.common.Ack;
import com.floragunn.signals.watch.common.HttpEndpointWhitelist;
import com.floragunn.signals.watch.result.ActionLog;
import com.floragunn.signals.watch.result.Status;
import com.floragunn.signals.watch.result.WatchLog;
import com.floragunn.signals.watch.result.WatchLogWriter;
import com.floragunn.signals.watch.severity.SeverityLevel;
import com.floragunn.signals.watch.severity.SeverityMapping;
import com.floragunn.signals.watch.state.ActionState;
import com.floragunn.signals.watch.state.NopActionState;
import com.floragunn.signals.watch.state.WatchState;
import com.floragunn.signals.watch.state.WatchStateWriter;

@DisallowConcurrentExecution
public class WatchRunner implements Job {
    private static final Logger log = LogManager.getLogger(WatchRunner.class);

    private final Watch watch;
    private final WatchExecutionContext ctx;
    private final WatchExecutionContextData contextData;
    private final Client client;

    private final ScriptService scriptService;
    private final WatchLogWriter watchLogWriter;
    private final WatchStateWriter<?> watchStateWriter;
    private final WatchState watchState;
    private final SignalsSettings signalsSettings;
    private final String nodeName;

    private final WatchLog watchLog = new WatchLog();
    private final SimulationMode simulationMode;
    private final GotoCheckSelector checkSelector;
    private final DiagnosticContext diagnosticContext;

    private SeverityLevel lastSeverityLevel;
    private SeverityLevel newSeverityLevel;

    private int attemptedActions = 0;
    private int executedActions = 0;
    private int throttledActions = 0;
    private int failedActions = 0;
    private int ackedActions = 0;
    private int attemptedResolveActions = 0;
    private int executedResolveActions = 0;
    private int failedResolveActions = 0;

    public WatchRunner(Watch watch, Client client, AccountRegistry accountRegistry, ScriptService scriptService, WatchLogWriter watchLogWriter,
            WatchStateWriter<?> watchStateWriter, DiagnosticContext diagnosticContext, WatchState watchState,
            ExecutionEnvironment executionEnvironment, SimulationMode simulationMode, NamedXContentRegistry xContentRegistry,
            SignalsSettings signalsSettings, String nodeName, GotoCheckSelector checkSelector, NestedValueMap input,
            TrustManagerRegistry trustManagerRegistry) {
        this.watch = watch;
        this.client = client;
        this.scriptService = scriptService;
        this.watchLogWriter = watchLogWriter;
        this.watchStateWriter = watchStateWriter;
        this.watchState = watchState;
        this.diagnosticContext = diagnosticContext;
        this.lastSeverityLevel = watchState != null ? watchState.getLastSeverityLevel() : null;
        this.contextData = new WatchExecutionContextData(new WatchExecutionContextData.WatchInfo(watch.getId(), watch.getTenant()));
        this.ctx = new WatchExecutionContext(client, scriptService, xContentRegistry, accountRegistry, executionEnvironment,
                ActionInvocationType.ALERT, this.contextData, watchState != null ? watchState.getLastExecutionContextData() : null, simulationMode,
                new HttpEndpointWhitelist(signalsSettings.getDynamicSettings().getAllowedHttpEndpoints()),
                signalsSettings.getDynamicSettings().getHttpProxyConfig(), signalsSettings.getDynamicSettings().getFrontendBaseUrl(), null,
                trustManagerRegistry);
        this.watchLog.setWatchId(watch.getId());
        this.watchLog.setWatchVersion(watch.getVersion());
        this.signalsSettings = signalsSettings;
        this.nodeName = nodeName;
        this.simulationMode = simulationMode;
        this.checkSelector = checkSelector;

        if (input != null) {
            this.contextData.getData().putAll(input);
        }
    }

    public Watch getWatch() {
        return watch;
    }

    public WatchExecutionContext getCtx() {
        return ctx;
    }

    public Client getClient() {
        return client;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            contextData.setTriggerInfo(new WatchExecutionContextData.TriggerInfo(context.getFireTime(), context.getScheduledFireTime(),
                    context.getPreviousFireTime(), context.getNextFireTime()));
            execute();
        } catch (WatchExecutionException e) {
            log.info("Error while executing " + watch, e);
            throw new JobExecutionException(e);
        }
    }

    public WatchLog execute() throws WatchExecutionException {
        try (DiagnosticContext.Handle h = diagnosticContext.pushActionStack("signals_watch:" + watch.getTenant() + "/" + watch.getId())) {

            if (log.isInfoEnabled()) {
                log.info("Running " + watch + "@" + watch.getVersion());
            }

            if (log.isDebugEnabled()) {
                log.debug("Current watch state: " + (watchState != null ? watchState.getCreationTime() : "-") + "\n"
                        + (watchState != null ? Strings.toString(watchState) : null));
            }

            boolean error = false;

            try {
                Instant executionStart = Instant.now();
                contextData.setExecutionTime(ZonedDateTime.ofInstant(executionStart, ZoneOffset.UTC));
                this.watchLog.setExecutionStart(Date.from(executionStart));
                this.watchLog.setActions(new ArrayList<ActionLog>(this.watch.getActions().size()));
                this.watchLog.setResolveActions(new ArrayList<ActionLog>(this.watch.getResolveActions().size()));
                this.watchLog.setTenant(watch.getTenant());

                if (this.signalsSettings.isIncludeNodeInWatchLogEnabled()) {
                    this.watchLog.setNode(nodeName);
                }

                if (!executeChecks()) {
                    return this.watchLog;
                }

                if (!executeSeverityMapping()) {
                    return this.watchLog;
                }

                executeActions();

                executeResolveActions();

                setWatchLogStatus();

                return watchLog;
            } catch (Exception e) {
                error = true;

                if (this.watchLog.getStatus() == null) {
                    this.watchLog.setStatus(new Status(Status.Code.EXECUTION_FAILED, e.toString()));
                }

                if (e instanceof WatchExecutionException) {
                    throw (WatchExecutionException) e;
                } else {
                    throw new WatchExecutionException("Error while executing " + watch, e, this.watchLog);
                }
            } finally {

                if (this.watchState != null) {
                    if (!error) {
                        this.watchState.setLastExecutionContextData(this.contextData);
                    }

                    this.watchState.setLastStatus(this.watchLog.getStatus());
                }

                if (this.watchStateWriter != null && this.watchState != null) {
                    this.watchStateWriter.put(watch.getId(), this.watchState);
                }

                this.watchLog.setExecutionFinished(new Date());

                this.watchLog.setData(contextData.getData().clone());
                this.watchLog.setRuntimeAttributes(contextData.clone());

                if (this.watchLogWriter != null) {
                    this.watchLogWriter.put(this.watchLog);
                }

                if (log.isInfoEnabled()) {
                    log.info("Finished " + watch + ": " + this.watchLog.getStatus());
                }
            }
        }
    }

    private boolean executeChecks() throws WatchExecutionException {
        for (Check check : watch.getChecks()) {
            try {
                if (this.checkSelector != null && !this.checkSelector.isSelected(check)) {
                    log.info("Skipping check " + check + " because of check selector " + checkSelector);
                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Before running " + check);
                }

                if (!check.execute(ctx)) {
                    afterNegativeTriageForAllActions();
                    this.watchLog.setStatus(new Status(Status.Code.NO_ACTION, "No action needed due to check " + check.getName()));
                    return false;
                }

                if (log.isDebugEnabled()) {
                    log.debug("After running " + check + "\n" + contextData.getData());
                }
            } catch (Exception e) {
                this.watchLog.setStatus(new Status(Status.Code.EXECUTION_FAILED, "Error while executing " + check + ": " + e.getMessage()));

                if (e instanceof WatchOperationExecutionException) {
                    this.watchLog.setError(((WatchOperationExecutionException) e).toErrorInfo());
                } else {
                    this.watchLog.setError(new WatchOperationExecutionException(e).toErrorInfo());
                }

                throw new WatchExecutionException("Error while executing " + check, e, this.watchLog);
            }
        }

        return true;
    }

    private void afterNegativeTriageForAllActions() {
        for (ActionInvoker action : watch.getActions()) {
            ActionState actionState = getActionState(action);
            actionState.afterNegativeTriage();
        }
    }

    private boolean executeSeverityMapping() throws WatchExecutionException {
        SeverityMapping severityMapping = this.watch.getSeverityMapping();

        if (severityMapping == null) {
            return true;
        }

        try {
            SeverityMapping.EvaluationResult severityLevelEvaluationResult = severityMapping.execute(ctx);

            if (log.isDebugEnabled()) {
                log.debug("Determined severity level: " + severityLevelEvaluationResult);
            }

            this.contextData.setSeverity(severityLevelEvaluationResult);

            this.newSeverityLevel = severityLevelEvaluationResult.getLevel();

            if (severityLevelEvaluationResult.getLevel() == SeverityLevel.NONE) {
                executeResolveActions();

                // TODO improve message

                afterNegativeTriageForAllActions();
                this.watchLog.setStatus(new Status(Status.Code.NO_ACTION, SeverityLevel.NONE, "No action needed because severity value "
                        + severityLevelEvaluationResult.getThreshold() + " is under threshold " + severityMapping.getFirstThreshold()));

                return false;
            }

            return true;

        } catch (Exception e) {
            this.watchLog.setStatus(new Status(Status.Code.EXECUTION_FAILED, "Error while executing severity mapping: " + e.getMessage()));

            if (e instanceof WatchOperationExecutionException) {
                this.watchLog.setError(((WatchOperationExecutionException) e).toErrorInfo());
            } else {
                this.watchLog.setError(new WatchOperationExecutionException(e).toErrorInfo());
            }

            throw new WatchExecutionException("Error while executing severity mapping", e, this.watchLog);
        }
    }

    private void executeActions() {

        if (watch.getActions().isEmpty()) {
            watchLog.setStatus(new Status(Status.Code.NO_ACTION, "No actions are configured"));
            return;
        }

        for (AlertAction action : watch.getActions()) {
            ActionLog actionLog = new ActionLog(action.getName() != null ? action.getName() : action.toString());
            watchLog.getActions().add(actionLog);

            ActionState actionState = getActionState(action);

            try {

                if (log.isDebugEnabled()) {
                    log.debug("Before running " + action);
                }

                actionLog.setExecutionStart(new Date());

                ActionState.BasicState basicActionState = actionState.beforeExecution(getThrottlePeriod(action));

                if (action.getSeverityLevels() != null && action.getSeverityLevels().size() != 0) {

                    if (newSeverityLevel == null) {
                        actionLog.setStatus(new Status(Status.Code.ACTION_FAILED,
                                "Action " + action.getName() + " is configured with severity levels, while watch does not define severity levels"));
                        attemptedActions++;
                        failedActions++;
                        continue;
                    }

                    // TODO extract this logic and make it unit-testable

                    if (action.getSeverityLevels().isGained(lastSeverityLevel, newSeverityLevel)) {
                        if (basicActionState == ActionState.BasicState.THROTTLED) {
                            basicActionState = ActionState.BasicState.EXECUTABLE;

                            if (log.isDebugEnabled()) {
                                log.debug("Unthrottling and executing " + action.getName() + " because configured severity level has been gained: "
                                        + lastSeverityLevel + " < " + action.getSeverityLevels() + " <= " + newSeverityLevel);
                            }

                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Executing " + action.getName() + " because configured severity level has been gained: " + lastSeverityLevel
                                        + " < " + action.getSeverityLevels() + " <= " + newSeverityLevel);
                            }
                        }
                    } else if (action.getSeverityLevels().getLowest().isHigherThan(newSeverityLevel)) {
                        if (log.isDebugEnabled()) {
                            log.debug(newSeverityLevel + " is lower than lowest level of " + action);
                        }

                        actionState.afterNegativeTriage();
                        actionLog.setStatus(new Status(Status.Code.NO_ACTION,
                                "No action because current severity is lower than severity configured for action: " + newSeverityLevel));
                        continue;
                    } else if (!action.getSeverityLevels().contains(newSeverityLevel)) {

                        if (log.isDebugEnabled()) {
                            log.debug("Not executing " + action + " because the configured severity levels don't contain " + newSeverityLevel);
                        }

                        actionLog.setStatus(
                                new Status(Status.Code.NO_ACTION, "No action because action is not configured for severity " + newSeverityLevel));
                        continue;
                    }
                }

                if (basicActionState == ActionState.BasicState.THROTTLED) {
                    actionLog.setStatus(new Status(Status.Code.ACTION_THROTTLED, null));
                    throttledActions++;
                    continue;
                }

                attemptedActions++;

                if (action.getForeach() == null) {

                    WatchExecutionContext ctx = this.prepareInputForAction(this.ctx, action, actionLog);

                    if (ctx == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Not executing " + action + " because the checks did not found the action to be eligible for execution");
                        }

                        actionState.afterNegativeTriage();
                        actionLog.setStatus(new Status(Status.Code.NO_ACTION, "No action due to check conditions"));
                        continue;
                    }

                    Ack acked = actionState.afterPositiveTriage();

                    if (acked != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Not executing " + action + " because it was already acked: " + acked);
                        }

                        actionLog.setStatus(new Status(Status.Code.ACKED, "Already acked"));
                        actionLog.setAck(acked);
                        ackedActions++;
                        continue;
                    }

                    if (simulationMode == SimulationMode.FOR_REAL || simulationMode == SimulationMode.SIMULATE_ACTIONS) {
                        ActionExecutionResult result = action.execute(ctx);

                        if (simulationMode == SimulationMode.FOR_REAL) {
                            actionLog.setStatus(new Status(Status.Code.ACTION_EXECUTED, null));
                        } else {
                            actionLog.setRequest(result.getRequest());
                            actionLog.setStatus(
                                    new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action was triggered in simulation mode"));
                        }
                    } else {
                        actionLog.setStatus(new Status(Status.Code.SIMULATED_ACTION_EXECUTED,
                                "Simulate mode: Action would have been triggered but was skipped."));
                    }

                    executedActions++;

                    actionState.afterSuccessfulExecution();
                } else {
                    executeForEachAction(action, actionState, actionLog);
                }
            } catch (CheckExecutionException e) {
                log.warn("Error while executing " + action + " of " + watch, e);
                actionLog.setStatus(new Status(Status.Code.ACTION_FAILED, "Check " + e.getCheckId() + " failed: " + e.getMessage()));
                actionLog.setError(e.toErrorInfo());

                failedActions++;
            } catch (Exception e) {
                log.warn("Error while executing " + action + " of " + watch, e);
                actionLog.setStatus(new Status(Status.Code.ACTION_FAILED, e.getMessage() != null ? e.getMessage() : e.toString()));

                if (e instanceof WatchOperationExecutionException) {
                    actionLog.setError(((WatchOperationExecutionException) e).toErrorInfo());
                } else {
                    actionLog.setError(new WatchOperationExecutionException(e).toErrorInfo());
                }

                actionState.setLastError(Instant.now());

                failedActions++;
            } finally {
                actionLog.setExecutionEnd(new Date());
                actionState.setLastStatus(actionLog.getStatus());

                if (log.isDebugEnabled()) {
                    log.debug("Finished " + action + ": " + actionLog.getStatus());
                }
            }
        }

    }

    private void executeForEachAction(AlertAction action, ActionState actionState, ActionLog actionLog) {

        SignalsObjectFunctionScript s = action.getForeach().getScriptFactory().newInstance(Collections.emptyMap(), this.ctx);

        Object collection = s.execute();

        if (!(collection instanceof Iterable)) {
            collection = Collections.singleton(collection);
        }

        List<ActionLog> elementLogs = new ArrayList<>(
                collection instanceof Collection ? Math.max(((Collection<?>) collection).size(), action.getForeachLimit()) : 100);

        actionLog.setElements(elementLogs);

        int totalElements = 0;
        int executedActions = 0;
        int errors = 0;

        for (Object elem : (Iterable<?>) collection) {
            if (totalElements >= action.getForeachLimit()) {
                break;
            }

            ActionLog elementLog = new ActionLog();
            elementLogs.add(elementLog);

            totalElements++;

            WatchExecutionContext ctx = this.ctx.clone();

            try {
                ctx.getContextData().setItem(NestedValueMap.copy(elem));
                ctx = this.prepareInputForAction(ctx, action, actionLog);

                if (ctx == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Not executing " + action + " because the checks did not found the action to be eligible for execution");
                    }

                    elementLog.setStatus(new Status(Status.Code.NO_ACTION, "No action due to check conditions"));
                    continue;
                }

                if (simulationMode == SimulationMode.FOR_REAL || simulationMode == SimulationMode.SIMULATE_ACTIONS) {
                    ActionExecutionResult result = action.execute(ctx);

                    if (simulationMode == SimulationMode.FOR_REAL) {
                        elementLog.setStatus(new Status(Status.Code.ACTION_EXECUTED, null));
                    } else {
                        elementLog.setRequest(result.getRequest());
                        elementLog.setStatus(
                                new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action was triggered in simulation mode"));
                    }
                } else {
                    elementLog.setStatus(
                            new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action would have been triggered but was skipped."));
                }

                executedActions++;
            } catch (CheckExecutionException e) {
                log.warn("Error while executing " + action + " of " + watch, e);
                elementLog.setStatus(new Status(Status.Code.ACTION_FAILED, "Check " + e.getCheckId() + " failed: " + e.getMessage()));
                elementLog.setError(e.toErrorInfo());
                errors++;
                continue;

            } catch (ActionExecutionException e) {
                log.warn("Error while executing " + action + " of " + watch, e);
                elementLog.setStatus(new Status(Status.Code.ACTION_FAILED, e.getMessage() != null ? e.getMessage() : e.toString()));

                if (e instanceof WatchOperationExecutionException) {
                    elementLog.setError(((WatchOperationExecutionException) e).toErrorInfo());
                } else {
                    elementLog.setError(new WatchOperationExecutionException(e).toErrorInfo());
                }

                errors++;
                continue;
            }
        }

        if (errors > 0) {
            actionLog.setStatus(new Status(Status.Code.ACTION_FAILED, "Action failed for " + errors + " of " + totalElements + " elements"));
            actionState.setLastError(Instant.now());

            failedActions++;
        } else if (executedActions == 0) {
            actionState.afterNegativeTriage();
            actionLog.setStatus(new Status(Status.Code.NO_ACTION, "No action due to check conditions"));
        } else if (executedActions == totalElements) {
            if (simulationMode == SimulationMode.FOR_REAL || simulationMode == SimulationMode.SIMULATE_ACTIONS) {
                actionLog.setStatus(new Status(Status.Code.ACTION_EXECUTED, "Action executed for all " + totalElements + " elements"));
            } else {
                actionLog.setStatus(new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action would have been triggered."));
            }

            this.executedActions++;

            actionState.afterSuccessfulExecution();
        } else {
            if (simulationMode == SimulationMode.FOR_REAL || simulationMode == SimulationMode.SIMULATE_ACTIONS) {
                actionLog.setStatus(
                        new Status(Status.Code.ACTION_EXECUTED, "Action executed for " + executedActions + " of " + totalElements + " elements"));
            } else {
                actionLog.setStatus(new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action would have been triggered."));
            }
            this.executedActions++;

            actionState.afterSuccessfulExecution();
        }

    }

    private void executeResolveActions() {
        WatchExecutionContext ctx = this.ctx.with(ActionInvocationType.RESOLVE);

        for (ResolveAction action : watch.getResolveActions()) {
            ActionLog actionLog = new ActionLog(action.getName() != null ? action.getName() : action.toString());
            watchLog.getResolveActions().add(actionLog);

            try {

                if (!action.getResolvesSeverityLevels().isLost(lastSeverityLevel, newSeverityLevel)) {
                    if (log.isDebugEnabled()) {
                        log.debug("Not running " + action + " because " + action.getResolvesSeverityLevels() + " is not in between "
                                + lastSeverityLevel + " and " + newSeverityLevel);
                    }

                    actionLog.setStatus(new Status(Status.Code.NO_ACTION, null));

                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Running " + action);
                }

                attemptedResolveActions++;

                actionLog.setExecutionStart(new Date());

                WatchExecutionContext actionCtx = this.prepareInputForAction(ctx, action, actionLog);

                if (actionCtx == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Not executing " + action + " because the checks did not found the action to be eligible for execution");
                    }

                    actionLog.setStatus(new Status(Status.Code.NO_ACTION, "No action due to check conditions"));
                    continue;
                }

                if (simulationMode == SimulationMode.FOR_REAL || simulationMode == SimulationMode.SIMULATE_ACTIONS) {
                    action.execute(actionCtx);
                    actionLog.setStatus(new Status(Status.Code.ACTION_EXECUTED, null));
                } else {
                    actionLog.setStatus(new Status(Status.Code.SIMULATED_ACTION_EXECUTED, "Simulate mode: Action would have been triggered."));
                }

                executedResolveActions++;
            } catch (Exception e) {
                log.warn("Error while executing " + action + " of " + watch, e);
                actionLog.setStatus(new Status(Status.Code.ACTION_FAILED, e.getMessage() != null ? e.getMessage() : e.toString()));

                if (e instanceof WatchOperationExecutionException) {
                    actionLog.setError(((WatchOperationExecutionException) e).toErrorInfo());
                } else {
                    actionLog.setError(new WatchOperationExecutionException(e).toErrorInfo());
                }

                failedResolveActions++;
            } finally {
                actionLog.setExecutionEnd(new Date());

                if (log.isDebugEnabled()) {
                    log.debug("Finished " + action + ": " + actionLog.getStatus());
                }
            }
        }
    }

    private void setWatchLogStatus() {

        String statusMessage = getStatusMessage();

        if (executedActions + executedResolveActions > 0) {
            watchLog.setStatus(new Status(Status.Code.ACTION_EXECUTED, newSeverityLevel, statusMessage));
        } else if (failedActions + executedResolveActions > 0) {
            watchLog.setStatus(new Status(Status.Code.ACTION_FAILED, newSeverityLevel, statusMessage));
        } else if (throttledActions > 0) {
            watchLog.setStatus(new Status(Status.Code.ACTION_THROTTLED, newSeverityLevel, statusMessage));
        } else if (ackedActions > 0) {
            watchLog.setStatus(new Status(Status.Code.ACKED, newSeverityLevel, statusMessage));
        } else {
            watchLog.setStatus(new Status(Status.Code.NO_ACTION, newSeverityLevel, statusMessage));
        }
    }

    private DurationExpression getThrottlePeriod(AlertAction action) {
        DurationExpression result = action.getThrottlePeriod();

        if (result == null) {
            result = watch.getThrottlePeriod();
        }

        DurationExpression lowerBound = signalsSettings.getThrottlePeriodLowerBound();

        if (lowerBound != null) {
            if (result == null || result.getActualDuration(0).compareTo(lowerBound.getActualDuration(0)) < 0) {
                result = lowerBound;
            }
        }

        if (result == null) {
            return signalsSettings.getDefaultThrottlePeriod();
        }

        return result;
    }

    private String getStatusMessage() {
        if (failedActions == 0 && throttledActions == 0 && executedActions == 0 && ackedActions == 0 && executedResolveActions == 0
                && failedResolveActions == 0) {
            return "No action needed to be executed due to their conditions";
        }

        if (failedActions == 0 && throttledActions == 0 && ackedActions == 0 && failedResolveActions == 0) {
            return "All actions have been executed";
        }

        if (failedActions + failedResolveActions != 0 && failedActions == attemptedActions && failedResolveActions == attemptedResolveActions) {
            return "All actions failed";
        }

        if (throttledActions == watch.getActions().size() && attemptedResolveActions == 0) {
            return "All actions have been throttled";
        }

        if (ackedActions == attemptedActions && attemptedResolveActions == 0) {
            return "All actions have been acknowledged before";
        }

        StringBuilder result = new StringBuilder();

        if (executedActions > 0) {
            result.append(executedActions + " actions have been executed");
        }

        if (throttledActions > 0) {
            if (result.length() > 0) {
                result.append("; ");
            }

            result.append(throttledActions + " actions were not considered because they are throttled");
        }

        if (ackedActions > 0) {
            if (result.length() > 0) {
                result.append("; ");
            }

            result.append(ackedActions + " actions were not executed because they have been acknowledged before");
        }

        if (failedActions > 0) {
            if (result.length() > 0) {
                result.append("; ");
            }

            result.append(failedActions + " actions failed");
        }

        if (executedResolveActions > 0) {
            if (result.length() > 0) {
                result.append("; ");
            }

            result.append(executedResolveActions + " resolve actions have been executed");
        }

        if (failedResolveActions > 0) {
            if (result.length() > 0) {
                result.append("; ");
            }

            result.append(failedResolveActions + " resolve actions failed");
        }

        return result.toString();
    }

    private WatchExecutionContext prepareInputForAction(WatchExecutionContext ctx, ActionInvoker action, ActionLog actionLog)
            throws CheckExecutionException {

        if (action.getChecks().isEmpty()) {
            return ctx.with(ctx.getContextData(), action);
        }

        WatchExecutionContextData actionContextData = ctx.getContextData().clone();

        try {

            ctx = ctx.with(actionContextData, action);

            for (Check input : action.getChecks()) {
                if (!input.execute(ctx)) {
                    return null;
                }
            }

            return ctx;
        } finally {
            if ((watch.isLogRuntimeData() || ctx.getExecutionEnvironment() == ExecutionEnvironment.TEST)
                    && !actionContextData.getData().equals(this.contextData.getData())) {
                actionLog.setData(actionContextData.getData());
            }
        }
    }

    private ActionState getActionState(ActionInvoker action) {
        if (this.watchState == null) {
            // Possibly in a manual execution or a unit test
            return new NopActionState();
        }

        if (action.getName() == null) {
            // Name is mandatory, so this should not happen
            return new NopActionState();
        }

        return this.watchState.getActionState(action.getName());
    }

}
