"""
Dagster sensors — monitors pipeline runs and sends email alerts on failure.
Monitors: velib_pipeline_job, velib_cleanup_job
"""
import os

import dagster as dg
import requests


def send_failure_email(run_id: str, job_name: str, error: str) -> None:
    """Sends a failure alert email via Resend API."""
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        return

    requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "from": "Dagster <alerts@julien-castellano.fr>",
            "to": [os.getenv("ALERT_EMAIL")],
            "subject": f"❌ Vélib Lakehouse — {job_name} Failed",
            "html": f"""
                <h2>Pipeline failure detected</h2>
                <p><strong>Job:</strong> {job_name}</p>
                <p><strong>Run ID:</strong> {run_id}</p>
                <p><strong>Error:</strong></p>
                <pre>{error}</pre>
            """,
        },
        timeout=10,
    )


@dg.run_failure_sensor(
    monitored_jobs=["velib_pipeline_job", "velib_cleanup_job"]
)
def velib_failure_sensor(context: dg.RunFailureSensorContext):
    """Triggers on failure of velib_pipeline_job or velib_cleanup_job."""
    send_failure_email(
        run_id=context.dagster_run.run_id,
        job_name=context.dagster_run.job_name,
        error=context.failure_event.message if context.failure_event else "Unknown error",
    )