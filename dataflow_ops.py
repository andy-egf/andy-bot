#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "google-cloud-dataflow-client>=0.8.0",
#     "google-cloud-logging>=3.0.0",
#     "rich>=13.0.0",
# ]
# ///
"""
Google Dataflow Operations CLI

Operations:
  - list-jobs: List the last 25 Dataflow jobs
  - wait: Wait for a job to complete (with polling)
  - wait-latest: Wait for the latest running job to complete
  - get-error: Get error details for a failed job
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import dataflow_v1beta3 as dataflow
from google.cloud import logging as cloud_logging
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

# Hardcoded configuration
PROJECT_ID = "egfl-main"
REGION = "europe-west2"  # London, UK


# Job state mappings
JOB_STATE_MAP = {
    0: "UNKNOWN",
    1: "STOPPED",
    2: "RUNNING",
    3: "DONE",
    4: "FAILED",
    5: "CANCELLED",
    6: "UPDATED",
    7: "DRAINING",
    8: "DRAINED",
    9: "PENDING",
    10: "CANCELLING",
    11: "QUEUED",
    12: "RESOURCE_CLEANING_UP",
}

# Terminal states - job is no longer running
TERMINAL_STATES = {"DONE", "FAILED", "CANCELLED", "UPDATED", "DRAINED", "STOPPED"}


def get_job_state_name(state_value: int) -> str:
    """Convert numeric job state to readable name."""
    return JOB_STATE_MAP.get(state_value, f"UNKNOWN({state_value})")


def format_timestamp(timestamp) -> str:
    """Format a protobuf timestamp to readable string."""
    if timestamp is None:
        return "N/A"
    try:
        if hasattr(timestamp, "seconds"):
            dt = datetime.fromtimestamp(timestamp.seconds, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return str(timestamp)
    except Exception:
        return str(timestamp)


def get_dataflow_client() -> dataflow.JobsV1Beta3Client:
    """Create and return a Dataflow jobs client."""
    return dataflow.JobsV1Beta3Client()


def list_jobs(limit: int = 25) -> None:
    """List the last N Dataflow jobs as JSON lines."""
    client = get_dataflow_client()

    try:
        request = dataflow.ListJobsRequest(
            project_id=PROJECT_ID,
            location=REGION,
            page_size=limit,
        )

        jobs = list(client.list_jobs(request=request))[:limit]

        for job in jobs:
            job_data = {
                "name": job.name or None,
                "id": job.id or None,
                "status": get_job_state_name(job.current_state),
                "type": job.type_.name if job.type_ else None,
                "created": format_timestamp(job.create_time),
                "state_time": format_timestamp(job.current_state_time),
            }
            print(json.dumps(job_data))

    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)


def get_job(job_id: str) -> Optional[dataflow.Job]:
    """Get a specific job by ID."""
    client = get_dataflow_client()

    try:
        request = dataflow.GetJobRequest(
            project_id=PROJECT_ID,
            location=REGION,
            job_id=job_id,
            view=dataflow.JobView.JOB_VIEW_ALL,
        )
        return client.get_job(request=request)
    except Exception as e:
        console.print(f"[red]Error getting job {job_id}: {e}[/red]")
        return None


def wait_for_job(
    job_id: str,
    poll_interval: int = 30,
    timeout: Optional[int] = None,
) -> None:
    """Wait for a job to reach a terminal state."""
    console.print(f"\n[bold]Waiting for job {job_id} to complete...[/bold]\n")

    start_time = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Checking job status...", total=None)

        while True:
            job = get_job(job_id)

            if job is None:
                console.print("[red]Failed to retrieve job. Exiting.[/red]")
                sys.exit(1)

            state_name = get_job_state_name(job.current_state)
            elapsed = int(time.time() - start_time)

            progress.update(
                task,
                description=f"Job state: [bold]{state_name}[/bold] (elapsed: {elapsed}s)",
            )

            if state_name in TERMINAL_STATES:
                progress.stop()
                console.print(f"\n[bold]Job reached terminal state: {state_name}[/bold]")

                if state_name == "DONE":
                    console.print("[green]Job completed successfully![/green]")
                    sys.exit(0)
                elif state_name == "FAILED":
                    console.print("[red]Job failed![/red]")
                    console.print(
                        f"\n[yellow]Run the following to get error details:[/yellow]"
                    )
                    console.print(
                        f"  uv run {__file__} get-error --job-id {job_id}"
                    )
                    sys.exit(1)
                elif state_name == "CANCELLED":
                    console.print("[yellow]Job was cancelled.[/yellow]")
                    sys.exit(1)
                else:
                    console.print(f"[dim]Job ended with state: {state_name}[/dim]")
                    sys.exit(0)

            # Check timeout
            if timeout and elapsed >= timeout:
                progress.stop()
                console.print(
                    f"\n[red]Timeout reached ({timeout}s). Job still in state: {state_name}[/red]"
                )
                sys.exit(1)

            time.sleep(poll_interval)


def get_job_errors_from_messages(job_id: str) -> list[dict]:
    """Get error messages from job messages API."""
    client = dataflow.MessagesV1Beta3Client()
    errors = []

    try:
        request = dataflow.ListJobMessagesRequest(
            project_id=PROJECT_ID,
            location=REGION,
            job_id=job_id,
            minimum_importance=dataflow.JobMessageImportance.JOB_MESSAGE_ERROR,
        )

        for message in client.list_job_messages(request=request).job_messages:
            errors.append(
                {
                    "time": format_timestamp(message.time),
                    "message": message.message_text,
                    "importance": message.message_importance.name
                    if message.message_importance
                    else "N/A",
                }
            )
    except Exception as e:
        console.print(f"[yellow]Warning: Could not fetch job messages: {e}[/yellow]")

    return errors


def get_job_errors_from_logs(job_id: str, limit: int = 50) -> list[dict]:
    """Get error logs from Cloud Logging for a Dataflow job."""
    errors = []

    try:
        logging_client = cloud_logging.Client(project=PROJECT_ID)

        # Filter for Dataflow job errors
        filter_str = (
            f'resource.type="dataflow_step" '
            f'resource.labels.job_id="{job_id}" '
            f'severity>=ERROR'
        )

        entries = logging_client.list_entries(
            filter_=filter_str,
            order_by=cloud_logging.DESCENDING,
            max_results=limit,
        )

        for entry in entries:
            payload = entry.payload
            if isinstance(payload, dict):
                message = payload.get("message", str(payload))
            else:
                message = str(payload)

            errors.append(
                {
                    "time": str(entry.timestamp) if entry.timestamp else "N/A",
                    "severity": entry.severity or "ERROR",
                    "message": message,
                    "worker": entry.resource.labels.get("step_id", "N/A")
                    if entry.resource
                    else "N/A",
                }
            )
    except Exception as e:
        console.print(f"[yellow]Warning: Could not fetch Cloud Logging entries: {e}[/yellow]")

    return errors


def get_latest_running_job() -> Optional[dataflow.Job]:
    """Get the most recent running or pending job."""
    client = get_dataflow_client()

    try:
        request = dataflow.ListJobsRequest(
            project_id=PROJECT_ID,
            location=REGION,
            page_size=50,
        )

        jobs = list(client.list_jobs(request=request))

        # Find the latest job that is running or pending
        running_states = {"RUNNING", "PENDING", "QUEUED", "DRAINING", "CANCELLING"}
        for job in jobs:
            state_name = get_job_state_name(job.current_state)
            if state_name in running_states:
                return job

        # If no running job found, return the most recent job
        return jobs[0] if jobs else None

    except Exception as e:
        console.print(f"[red]Error listing jobs: {e}[/red]")
        return None


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def wait_for_latest_job() -> None:
    """Find the latest running job and wait for it to complete.

    Uses hardcoded values:
    - Poll interval: 60 seconds (1 minute)
    - Timeout: 2400 seconds (40 minutes)
    """
    poll_interval = 60  # 1 minute
    timeout = 2400  # 40 minutes

    console.print("\n[bold]Finding latest running Dataflow job...[/bold]\n")

    job = get_latest_running_job()

    if job is None:
        console.print("[yellow]No jobs found.[/yellow]")
        sys.exit(1)

    job_id = job.id
    job_name = job.name
    state_name = get_job_state_name(job.current_state)

    console.print(f"[bold]Job Name:[/bold] {job_name}")
    console.print(f"[bold]Job ID:[/bold] {job_id}")
    console.print(f"[bold]Current State:[/bold] {state_name}")
    console.print(f"[bold]Created:[/bold] {format_timestamp(job.create_time)}")

    # If already in terminal state, just print summary
    if state_name in TERMINAL_STATES:
        console.print(f"\n[yellow]Job already in terminal state: {state_name}[/yellow]")
        _print_job_summary(job)
        sys.exit(0 if state_name == "DONE" else 1)

    console.print(f"\n[bold]Waiting for job to complete (polling every 1m, timeout 40m)...[/bold]\n")

    start_time = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Checking job status...", total=None)

        while True:
            job = get_job(job_id)

            if job is None:
                console.print("[red]Failed to retrieve job. Exiting.[/red]")
                sys.exit(1)

            state_name = get_job_state_name(job.current_state)
            elapsed = int(time.time() - start_time)

            progress.update(
                task,
                description=f"Job state: [bold]{state_name}[/bold] (elapsed: {format_duration(elapsed)})",
            )

            if state_name in TERMINAL_STATES:
                progress.stop()
                _print_job_summary(job)

                if state_name == "DONE":
                    sys.exit(0)
                else:
                    sys.exit(1)

            # Check timeout
            if elapsed >= timeout:
                progress.stop()
                console.print(
                    f"\n[red]Timeout reached (40 minutes). Job still in state: {state_name}[/red]"
                )
                sys.exit(1)

            time.sleep(poll_interval)


def _print_job_summary(job: dataflow.Job) -> None:
    """Print a summary of the completed job."""
    state_name = get_job_state_name(job.current_state)

    # Calculate total duration
    total_time = "N/A"
    if job.create_time and job.current_state_time:
        try:
            start_ts = job.create_time.seconds
            end_ts = job.current_state_time.seconds
            duration_secs = end_ts - start_ts
            total_time = format_duration(duration_secs)
        except Exception:
            pass

    console.print("\n" + "=" * 50)
    console.print("[bold]Job Completed[/bold]")
    console.print("=" * 50)

    # Status with color
    if state_name == "DONE":
        status_display = f"[green]{state_name}[/green]"
    elif state_name == "FAILED":
        status_display = f"[red]{state_name}[/red]"
    else:
        status_display = f"[yellow]{state_name}[/yellow]"

    console.print(f"[bold]Status:[/bold]     {status_display}")
    console.print(f"[bold]ID:[/bold]         {job.id}")
    console.print(f"[bold]Name:[/bold]       {job.name}")
    console.print(f"[bold]Total Time:[/bold] {total_time}")
    console.print("=" * 50 + "\n")


def get_error(job_id: str) -> None:
    """Get detailed error information for a failed job."""
    console.print(f"\n[bold]Fetching error details for job {job_id}...[/bold]\n")

    # First, get the job to confirm it exists and check its state
    job = get_job(job_id)

    if job is None:
        console.print("[red]Could not retrieve job.[/red]")
        sys.exit(1)

    state_name = get_job_state_name(job.current_state)

    console.print(f"[bold]Job Name:[/bold] {job.name}")
    console.print(f"[bold]Job ID:[/bold] {job.id}")
    console.print(f"[bold]State:[/bold] {state_name}")
    console.print(f"[bold]Created:[/bold] {format_timestamp(job.create_time)}")
    console.print(f"[bold]State Time:[/bold] {format_timestamp(job.current_state_time)}")

    if state_name != "FAILED":
        console.print(
            f"\n[yellow]Note: Job is not in FAILED state (current: {state_name})[/yellow]"
        )

    # Get errors from job messages API
    console.print("\n[bold cyan]--- Errors from Job Messages API ---[/bold cyan]\n")
    job_errors = get_job_errors_from_messages(job_id)

    if job_errors:
        for i, error in enumerate(job_errors, 1):
            console.print(f"[red]Error {i}:[/red]")
            console.print(f"  [dim]Time:[/dim] {error['time']}")
            console.print(f"  [dim]Importance:[/dim] {error['importance']}")
            console.print(f"  [bold]Message:[/bold] {error['message']}")
            console.print()
    else:
        console.print("[dim]No errors found in job messages.[/dim]")

    # Get errors from Cloud Logging
    console.print("\n[bold cyan]--- Errors from Cloud Logging ---[/bold cyan]\n")
    log_errors = get_job_errors_from_logs(job_id)

    if log_errors:
        for i, error in enumerate(log_errors[:20], 1):  # Limit display to 20
            console.print(f"[red]Log Entry {i}:[/red]")
            console.print(f"  [dim]Time:[/dim] {error['time']}")
            console.print(f"  [dim]Severity:[/dim] {error['severity']}")
            console.print(f"  [dim]Worker/Step:[/dim] {error['worker']}")
            console.print(f"  [bold]Message:[/bold] {error['message'][:500]}")
            if len(error["message"]) > 500:
                console.print("  [dim]... (message truncated)[/dim]")
            console.print()

        if len(log_errors) > 20:
            console.print(
                f"[dim]Showing 20 of {len(log_errors)} log entries. "
                f"Check Cloud Console for full logs.[/dim]"
            )
    else:
        console.print("[dim]No errors found in Cloud Logging.[/dim]")

    # Print helpful links
    console.print("\n[bold]Useful Links:[/bold]")
    console.print(
        f"  Dataflow Console: https://console.cloud.google.com/dataflow/jobs/{REGION}/{job_id}?project={PROJECT_ID}"
    )
    console.print(
        f"  Logs Explorer: https://console.cloud.google.com/logs/query;query=resource.type%3D%22dataflow_step%22%0Aresource.labels.job_id%3D%22{job_id}%22?project={PROJECT_ID}"
    )


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description=f"Google Dataflow Operations CLI (project: {PROJECT_ID}, region: {REGION})",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List last 25 jobs
  %(prog)s list-jobs

  # Wait for a job to complete
  %(prog)s wait --job-id 2024-01-15_abc123

  # Get error details for a failed job
  %(prog)s get-error --job-id 2024-01-15_abc123
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # list-jobs command
    list_parser = subparsers.add_parser("list-jobs", help="List recent Dataflow jobs")
    list_parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=25,
        help="Number of jobs to list (default: 25)",
    )

    # wait command
    wait_parser = subparsers.add_parser("wait", help="Wait for a job to complete")
    wait_parser.add_argument(
        "--job-id",
        "-j",
        required=True,
        help="Dataflow job ID to wait for",
    )
    wait_parser.add_argument(
        "--poll-interval",
        "-i",
        type=int,
        default=30,
        help="Polling interval in seconds (default: 30)",
    )
    wait_parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=None,
        help="Maximum time to wait in seconds (default: no timeout)",
    )

    # get-error command
    error_parser = subparsers.add_parser(
        "get-error", help="Get error details for a failed job"
    )
    error_parser.add_argument(
        "--job-id",
        "-j",
        required=True,
        help="Dataflow job ID to get errors for",
    )

    # wait-latest command
    subparsers.add_parser(
        "wait-latest", help="Wait for the latest running job to complete (polls every 1m, 40m timeout)"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "list-jobs":
        list_jobs(args.limit)
    elif args.command == "wait":
        wait_for_job(
            args.job_id,
            args.poll_interval,
            args.timeout,
        )
    elif args.command == "get-error":
        get_error(args.job_id)
    elif args.command == "wait-latest":
        wait_for_latest_job()


if __name__ == "__main__":
    main()
