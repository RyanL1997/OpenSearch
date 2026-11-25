/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Node-local notifications of workload management acting on a search.
 *
 * Listeners run synchronously on the thread that rejects or cancels. They must not block,
 * must not issue searches, and must not rely on request context being available: a cancellation
 * is raised from the workload management monitoring thread rather than from the request's own.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class WorkloadGroupActionNotifier {
    private static final Logger logger = LogManager.getLogger(WorkloadGroupActionNotifier.class);
    private final CopyOnWriteArrayList<Consumer<Action>> listeners = new CopyOnWriteArrayList<>();

    /**
     * What workload management did to the search.
     */
    @ExperimentalApi
    public enum Kind {
        /** Admission was refused; the search never ran. */
        REJECTED,
        /** The search was running and its task was cancelled. */
        CANCELLED
    }

    /**
     * A single node-local workload management action.
     *
     * @param kind            whether the search was refused admission or cancelled while running
     * @param workloadGroupId the workload group the search was assigned to
     * @param resiliencyMode  the group's mode at the time, one of {@code SOFT}, {@code ENFORCED} or {@code MONITOR}
     * @param resourceType    the resource that was breaching, or null when the action was not attributed to one
     * @param threshold       the group's effective limit for that resource, or 0 when not applicable
     * @param observedUsage   the usage observed against that limit, or 0 when not applicable
     * @param taskId          the task that was cancelled, or -1 for a rejection, where no task exists yet
     */
    @ExperimentalApi
    public record Action(Kind kind, String workloadGroupId, String resiliencyMode, String resourceType, double threshold,
        double observedUsage, long taskId) {
    }

    public void addListener(Consumer<Action> listener) {
        listeners.addIfAbsent(Objects.requireNonNull(listener));
    }

    public void removeListener(Consumer<Action> listener) {
        listeners.remove(listener);
    }

    /**
     * Notifies listeners that a search was refused admission to a workload group.
     */
    public void notifyRejected(String workloadGroupId, String resiliencyMode, String resourceType, double threshold, double observedUsage) {
        if (listeners.isEmpty()) {
            return;
        }
        notify(new Action(Kind.REJECTED, workloadGroupId, resiliencyMode, resourceType, threshold, observedUsage, -1L));
    }

    /**
     * Notifies listeners that a running search was cancelled by workload management.
     */
    public void notifyCancelled(String workloadGroupId, long taskId) {
        if (listeners.isEmpty()) {
            return;
        }
        notify(new Action(Kind.CANCELLED, workloadGroupId, null, null, 0d, 0d, taskId));
    }

    private void notify(Action action) {
        for (Consumer<Action> listener : listeners) {
            try {
                listener.accept(action);
            } catch (RuntimeException e) {
                logger.warn("Workload group action listener failed", e);
            }
        }
    }
}
