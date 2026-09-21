/**
 * Types and interfaces for the Project Nox Work-Oriented Scheduler.
 *
 * Defines priority lanes, canonical work lifecycle states, active set structures,
 * persistent watermarks, admission configuration, and explainability telemetry.
 */
export var SchedulerLane;
(function (SchedulerLane) {
    SchedulerLane["P0_FRESH_RELEASE"] = "P0_FRESH_RELEASE";
    SchedulerLane["P1_CRITICAL_GAP"] = "P1_CRITICAL_GAP";
    SchedulerLane["P1_BACKFILL"] = "P1_BACKFILL";
    SchedulerLane["P2_ACTIVE_NEW_WORK"] = "P2_ACTIVE_NEW_WORK";
    SchedulerLane["P3_DISCOVERY"] = "P3_DISCOVERY";
    SchedulerLane["FALLBACK"] = "FALLBACK";
})(SchedulerLane || (SchedulerLane = {}));
