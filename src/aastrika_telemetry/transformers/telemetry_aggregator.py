"""Telemetry data aggregator - aggregates telemetry events."""

from collections import defaultdict
from aastrika_telemetry.models.events import TelemetryEvent
from aastrika_telemetry.models.summary import TelemetrySummary
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TelemetryAggregator:
    """Aggregate telemetry events from Elasticsearch format to PostgreSQL format."""

    @staticmethod
    def transform(telemetry_events: list[TelemetryEvent]) -> list[TelemetrySummary]:
        """
        Transform Elasticsearch documents to PostgreSQL-ready format.

        Args:
            telemetry_events: List of telemetry events from Elasticsearch

        Returns:
            List of transformed records ready for PostgreSQL
        """
        transformed = []

        # Group events by (session_id, content_id, course_id, user_id)
        composite_groups = defaultdict(list)

        for event in telemetry_events:
            # Skip events without required fields
            if not event.context or not event.context.sid or not event.actor or not event.actor.id:
                continue

            # Extract content_id and course_id
            content_id = None
            course_id = None

            if event.object and event.object.id:
                content_id = event.object.id

                # Extract course_id from rollup.l1
                if hasattr(event.object, "rollup") and event.object.rollup:
                    if isinstance(event.object.rollup, dict):
                        course_id = event.object.rollup.get("l1")
                    elif hasattr(event.object.rollup, "l1"):
                        course_id = event.object.rollup.l1

            # Skip if we don't have content_id or course_id
            if not content_id or not course_id:
                continue

            # Create composite key: (session_id, content_id, course_id, user_id)
            composite_key = (event.context.sid, content_id, course_id, event.actor.id)
            composite_groups[composite_key].append(event)




        summaries = []
        loop_count = 0
        negative_duration_count = 0

        for (
            session_id,
            content_id,
            course_id,
            user_id,
        ), group_events in composite_groups.items():
            loop_count += 1

            # Sort events by timestamp
            group_events.sort(key=lambda e: e.ets)

            # REQUIRED: Find first START event (earliest)
            start_event = next((e for e in group_events if e.eid == "START"), None)

            # Skip this group if no START event
            if not start_event:
                continue

            start_ets = start_event.ets

            # Find last END event that comes AFTER the start_event
            end_event = None
            for e in reversed(group_events):
                if e.eid == "END" and e.ets >= start_ets:
                    end_event = e
                    break

            if end_event:
                end_ets = end_event.ets
                end_imputed = False
            else:
                # No END event after START, use last event in this group
                end_ets = group_events[-1].ets
                end_imputed = True


            # Calculate duration for this combination
            duration_sec = (end_ets - start_ets)


            # Log negative duration records for investigation
            if duration_sec <= 0:
                logger.error(
                    f"Negative duration detected: "
                    f"session_id={session_id}, content_id={content_id}, "
                    f"course_id={course_id}, user_id={user_id}, "
                    f"duration_sec={duration_sec:.2f}, "
                    f"start_ets={start_ets}, end_ets={end_ets}, "
                    f"end_imputed={end_imputed}, "
                    f"num_events={len(group_events)}, "
                    f"event_ids=[{', '.join(e.eid for e in group_events)}], "
                    f"event_timestamps=[{', '.join(str(e.ets) for e in group_events)}]"
                )
                print("_" * 80)
                # Skip negative duration records
                negative_duration_count += 1
                continue


            # # Generate unique mid: SESCNT_DDMMYYYY_sessionId_contentId_courseId
            date_str = datetime.now().strftime("%d%m%Y")
            unique_mid = f"SESCNT_{date_str}_{session_id}_{content_id}_{course_id}"

            platform_id = None
            if start_event.context and start_event.context.pdata:
                platform_id = start_event.context.pdata.get("id")

            channel_id = None
            if start_event.context and start_event.context.channel:
                channel_id = start_event.context.channel

            # # Populate TelemetrySummary object
            session_content_summary = TelemetrySummary(
                user_id=user_id,
                session_id=session_id,
                content_id=content_id,
                course_id=course_id,
                mid=unique_mid,
                start_ets=start_ets,
                end_ets=end_ets,
                start_imputed=False,  # We always have START (required)
                end_imputed=end_imputed,
                total_time_duration=duration_sec,
                platform_id=platform_id,
                channel_id=channel_id,
            )

            summaries.append(session_content_summary)

        logger.info(f"<<<<<<<<<<<<<<< NEGATIVE RECORDS SKIPPED: {negative_duration_count} >>>>>>>>>>>>>>")
        logger.info(f"<<<<<<<<<<<<<<< TOTAL RECORD COUNT: {loop_count} >>>>>>>>>>>>>>")
        return summaries
