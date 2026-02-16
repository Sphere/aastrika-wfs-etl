import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from aastrika_telemetry.config.settings import app_config

logger = logging.getLogger(__name__)

LABEL_MAP = {
    "status": "Status",
    "es_index": "ES Index",
    "duration": "Duration (hh:mm:ss)",
    "sessions_processed": "Sessions Processed",
    "total_events": "Total Events",
    "valid_events": "Valid Events",
    "skipped_events": "Skipped Events",
    "valid_summaries": "Valid Summaries",
    "non_positive_skipped": "Non-positive Summaries",
    "loaded_to_db": "Loaded to DB",
}


def _build_html_body(stats: dict) -> str:
    """Build HTML email body from pipeline stats dict."""
    is_success = stats.get("status") == "SUCCESS"
    status_color = "#28a745" if is_success else "#dc3545"
    status_icon = "&#9989;" if is_success else "&#10060;"

    rows = ""
    for key, label in LABEL_MAP.items():
        value = stats.get(key, "N/A")
        if key == "status":
            value = f'<span style="color:{status_color};font-weight:bold;">{status_icon} {value}</span>'
        rows += f"""
        <tr>
            <td style="padding:8px 12px;border:1px solid #ddd;background:#f8f9fa;font-weight:600;">{label}</td>
            <td style="padding:8px 12px;border:1px solid #ddd;">{value}</td>
        </tr>"""

    return f"""
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;">
        <h2 style="color:{status_color};">Telemetry WFS Pipeline Report</h2>
        <table style="border-collapse:collapse;width:100%;max-width:500px;">
            <thead>
                <tr style="background:#343a40;color:#fff;">
                    <th style="padding:10px 12px;text-align:left;border:1px solid #ddd;">Metric</th>
                    <th style="padding:10px 12px;text-align:left;border:1px solid #ddd;">Value</th>
                </tr>
            </thead>
            <tbody>{rows}
            </tbody>
        </table>
        <br>
        <p style="font-size:12px;color:#888;">This is an automated email from the Aastrika Telemetry ETL Service.</p>
    </body>
    </html>
    """


def _build_html_error_body(error_msg: str) -> str:
    """Build HTML email body for pipeline failure."""
    return f"""
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;">
        <h2 style="color:#dc3545;">&#10060; Telemetry WFS Pipeline - FAILED</h2>
        <table style="border-collapse:collapse;width:100%;max-width:600px;">
            <tr>
                <td style="padding:10px 12px;border:1px solid #ddd;background:#f8f9fa;font-weight:600;">Status</td>
                <td style="padding:10px 12px;border:1px solid #ddd;color:#dc3545;font-weight:bold;">FAILED</td>
            </tr>
            <tr>
                <td style="padding:10px 12px;border:1px solid #ddd;background:#f8f9fa;font-weight:600;">Error</td>
                <td style="padding:10px 12px;border:1px solid #ddd;color:#dc3545;">{error_msg}</td>
            </tr>
        </table>
        <br>
        <p style="font-size:12px;color:#888;">This is an automated email from the Aastrika Telemetry ETL Service.</p>
    </body>
    </html>
    """


def send_email(subject: str, body: str | dict) -> bool:
    """Send email via AWS SES.

    Args:
        subject: Email subject line
        body: Plain text string or dict of pipeline stats (rendered as HTML table)

    Returns True if sent successfully, False otherwise.
    Email failure will never crash the application.
    """
    if not app_config.ses_sender_email or not app_config.ses_recipient_email:
        logger.warning("Email not configured. Skipping email notification.")
        return False

    if not app_config.aws_access_key_id or not app_config.aws_secret_access_key:
        logger.warning("AWS credentials not configured. Skipping email notification.")
        return False

    try:
        ses = boto3.client(
            "ses",
            region_name=app_config.aws_region,
            aws_access_key_id=app_config.aws_access_key_id,
            aws_secret_access_key=app_config.aws_secret_access_key,
        )

        recipients = [
            email.strip()
            for email in app_config.ses_recipient_email.split(",")
            if email.strip()
        ]

        logger.info("Sending email: %s to %s", subject, recipients)

        if isinstance(body, dict):
            email_body = {"Html": {"Data": _build_html_body(body)}}
        else:
            email_body = {"Html": {"Data": _build_html_error_body(body)}}

        ses.send_email(
            Source=app_config.ses_sender_email,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject},
                "Body": email_body,
            },
        )
        logger.info("Email sent successfully: %s", subject)
        return True

    except ClientError as e:
        logger.error("SES email failed: %s", e.response["Error"]["Message"])
        return False
    except NoCredentialsError:
        logger.error("AWS credentials are invalid.")
        return False
    except Exception as e:
        logger.error("Unexpected error sending email: %s", str(e))
        return False
