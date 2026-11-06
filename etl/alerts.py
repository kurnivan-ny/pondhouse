"""Failure alerts: POST a message to the webhook in ALERT_WEBHOOK_URL.

The payload works for Slack ("text") and Discord ("content"). With no URL set, the
alert is only logged. Sending never raises: a broken webhook must not fail a pipeline
that is otherwise fine, or hide the failure being reported.
"""
import json
import logging
import os
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def send_alert(message, timeout=10):
    """Send one alert. Returns True if the webhook accepted it."""
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        logger.warning("ALERT_WEBHOOK_URL is not set; alert not sent: %s", message)
        return False

    try:
        # Building the request is inside the try: a malformed URL raises here.
        request = urllib.request.Request(
            url,
            data=json.dumps({"text": message, "content": message}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout):
            return True
    except (urllib.error.URLError, OSError, ValueError) as e:
        # Only the reason: the URL itself is a secret.
        logger.error("could not send alert (%s): %s", type(e).__name__, message)
        return False
