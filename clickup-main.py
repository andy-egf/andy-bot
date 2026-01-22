#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "httpx",
#     "python-dotenv",
# ]
# ///
"""
ClickUp CLI - Unified tool for managing ClickUp tickets and sprints.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

import httpx
from dotenv import load_dotenv

load_dotenv()

CLICKUP_API_KEY = os.getenv("CLICKUP_API_KEY")
BASE_URL = "https://api.clickup.com/api/v2"


def get_headers() -> dict:
    return {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json",
    }


def api_get(endpoint: str, params: dict = None) -> dict:
    response = httpx.get(
        f"{BASE_URL}{endpoint}",
        headers=get_headers(),
        params=params,
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


def api_put(endpoint: str, data: dict) -> dict:
    response = httpx.put(
        f"{BASE_URL}{endpoint}",
        headers=get_headers(),
        json=data,
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


def api_post(endpoint: str, data: dict) -> dict:
    response = httpx.post(
        f"{BASE_URL}{endpoint}",
        headers=get_headers(),
        json=data,
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


def get_user() -> dict:
    return api_get("/user").get("user", {})


def get_teams() -> list:
    return api_get("/team").get("teams", [])


def get_spaces(team_id: str) -> list:
    return api_get(f"/team/{team_id}/space").get("spaces", [])


def get_folders(space_id: str) -> list:
    return api_get(f"/space/{space_id}/folder").get("folders", [])


def get_lists(folder_id: str) -> list:
    return api_get(f"/folder/{folder_id}/list").get("lists", [])


def get_tasks(list_id: str, assignee_id: str = None) -> list:
    params = {
        "include_closed": "false",
        "subtasks": "true",
    }
    if assignee_id:
        params["assignees[]"] = assignee_id
    return api_get(f"/list/{list_id}/task", params=params).get("tasks", [])


def get_task_details(task_id: str) -> dict:
    return api_get(f"/task/{task_id}")


def get_task_comments(task_id: str) -> list:
    return api_get(f"/task/{task_id}/comment").get("comments", [])


def get_task_by_custom_id(team_id: str, custom_id: str) -> dict | None:
    params = {
        "custom_task_ids": "true",
        "team_id": team_id,
    }
    try:
        return api_get(f"/task/{custom_id}", params=params)
    except httpx.HTTPStatusError:
        return None


def get_team_tasks(team_id: str, date_updated_gt: int, date_updated_lt: int) -> list:
    params = {
        "date_updated_gt": date_updated_gt,
        "date_updated_lt": date_updated_lt,
        "include_closed": "true",
        "subtasks": "true",
        "page": 0,
    }
    all_tasks = []
    while True:
        result = api_get(f"/team/{team_id}/task", params=params)
        tasks = result.get("tasks", [])
        if not tasks:
            break
        all_tasks.extend(tasks)
        if result.get("last_page", True):
            break
        params["page"] += 1
    return all_tasks


def update_task_status(task_id: str, status: str) -> dict:
    return api_put(f"/task/{task_id}", {"status": status})


def add_comment(task_id: str, comment_text: str) -> dict:
    return api_post(f"/task/{task_id}/comment", {"comment_text": comment_text})


def add_task_to_list(list_id: str, task_id: str) -> dict:
    """Add a task to a list (move to sprint)."""
    response = httpx.post(
        f"{BASE_URL}/list/{list_id}/task/{task_id}",
        headers=get_headers(),
        timeout=30.0
    )
    response.raise_for_status()
    return response.json()


def get_current_sprint_list() -> tuple[dict, str] | None:
    """Find the current sprint list and return (list_info, space_name).

    Looks for a sprint list with 'Sprint' and a number in the name,
    excluding backlogs.
    """
    teams = get_teams()
    for team in teams:
        team_id = team.get("id")
        spaces = get_spaces(team_id)
        for space in spaces:
            space_id = space.get("id")
            space_name = space.get("name")
            folders = get_folders(space_id)
            sprint_folder = find_sprint_folder(folders)
            if sprint_folder:
                sprint_lists = get_lists(sprint_folder.get("id"))
                # Find active sprint (not backlog)
                for sprint_list in sprint_lists:
                    list_name = sprint_list.get("name", "").lower()
                    # Skip backlogs, look for numbered sprints
                    if "backlog" in list_name:
                        continue
                    if "sprint" in list_name:
                        return sprint_list, space_name
                # Fallback to first non-backlog list
                for sprint_list in sprint_lists:
                    if "backlog" not in sprint_list.get("name", "").lower():
                        return sprint_list, space_name
    return None


def format_timestamp(ms: int | str | None) -> str | None:
    if not ms:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000)
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def format_timestamp_readable(ms: int | str | None) -> str:
    if not ms:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return "N/A"


def extract_comment_text(comment_text: list | str) -> str:
    if isinstance(comment_text, str):
        return comment_text
    if isinstance(comment_text, list):
        parts = []
        for item in comment_text:
            if isinstance(item, dict):
                parts.append(item.get("text", ""))
            elif isinstance(item, str):
                parts.append(item)
        return "".join(parts)
    return ""


def find_sprint_folder(folders: list) -> dict | None:
    priority_names = ["tech sprint", "current sprint", "sprint"]
    for priority_name in priority_names:
        for folder in folders:
            if priority_name in folder.get("name", "").lower():
                return folder
    return None


def lookup_task(ticket_number: str) -> tuple[dict, str]:
    teams = get_teams()
    if not teams:
        print("Error: No workspaces found")
        sys.exit(1)
    team_id = teams[0].get("id")
    task = get_task_by_custom_id(team_id, ticket_number)
    if not task:
        print(f"Error: Ticket {ticket_number} not found")
        sys.exit(1)
    return task, team_id


def get_previous_work_day() -> datetime:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    weekday = today.weekday()
    days_back = {
        0: 4,  # Monday → Thursday
        1: 1,  # Tuesday → Monday
        2: 1,  # Wednesday → Tuesday
        3: 1,  # Thursday → Wednesday
        4: 1,  # Friday → Thursday
        5: 2,  # Saturday → Thursday
        6: 3,  # Sunday → Thursday
    }
    return today - timedelta(days=days_back[weekday])


def get_work_day_range() -> tuple[int, int, str]:
    prev_work_day = get_previous_work_day()
    day_end = prev_work_day + timedelta(days=1) - timedelta(seconds=1)
    start_ms = int(prev_work_day.timestamp() * 1000)
    end_ms = int(day_end.timestamp() * 1000)
    date_str = prev_work_day.strftime("%Y-%m-%d")
    return start_ms, end_ms, date_str


# =============================================================================
# Commands
# =============================================================================

def cmd_sprint(args):
    """Export current sprint tickets to JSON."""
    print("=" * 60)
    print("ClickUp Current Sprint - Export to JSON")
    print("=" * 60)

    print("\nFetching user info...")
    user = get_user()
    user_id = user.get("id")
    username = user.get("username", "Unknown")
    print(f"  User: {username} (ID: {user_id})")

    print("\nFetching workspaces...")
    teams = get_teams()

    all_exported_tasks = []
    sprint_info = {}

    for team in teams:
        team_id = team.get("id")
        spaces = get_spaces(team_id)

        for space in spaces:
            space_id = space.get("id")
            space_name = space.get("name")
            folders = get_folders(space_id)
            sprint_folder = find_sprint_folder(folders)

            if sprint_folder:
                sprint_name = sprint_folder.get("name")
                sprint_id = sprint_folder.get("id")

                print(f"\nFound sprint: {sprint_name} in {space_name}")

                sprint_lists = get_lists(sprint_id)

                for lst in sprint_lists:
                    list_name = lst.get("name")
                    list_id = lst.get("id")

                    tasks = get_tasks(list_id, user_id)
                    open_tasks = [
                        t for t in tasks
                        if t.get("status", {}).get("type") != "closed"
                    ]

                    if open_tasks:
                        print(f"  Processing {len(open_tasks)} tasks from '{list_name}'...")
                        sprint_info["list_name"] = list_name

                    for task in open_tasks:
                        task_id = task.get("id")
                        print(f"    Fetching details for: {task.get('name', '')[:40]}...")

                        task_details = get_task_details(task_id)
                        comments = get_task_comments(task_id)

                        description = task_details.get("description", "") or ""
                        formatted_comments = []
                        for comment in comments:
                            commenter = comment.get("user", {})
                            formatted_comments.append({
                                "commenter": commenter.get("username", "Unknown"),
                                "text": extract_comment_text(comment.get("comment_text", "")),
                                "timestamp": format_timestamp(comment.get("date")),
                            })

                        ticket_number = task_details.get("custom_id") or task.get("custom_id")
                        tid = task_details.get("team_id") or task.get("team_id")
                        if ticket_number and tid:
                            friendly_url = f"https://app.clickup.com/t/{tid}/{ticket_number}"
                        else:
                            friendly_url = task.get("url", "")

                        all_exported_tasks.append({
                            "id": task.get("id"),
                            "ticket_number": ticket_number,
                            "title": task.get("name", ""),
                            "description": description,
                            "status": task.get("status", {}).get("status", ""),
                            "priority": task.get("priority", {}).get("priority") if task.get("priority") else None,
                            "due_date": format_timestamp(task.get("due_date")),
                            "url": friendly_url,
                            "comments": formatted_comments,
                        })

    export_data = {
        "exported_at": datetime.now().isoformat(),
        "user": username,
        "sprint": sprint_info.get("list_name", "Unknown"),
        "total_tasks": len(all_exported_tasks),
        "tasks": all_exported_tasks,
    }

    output_file = args.output
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print(f"Exported {len(all_exported_tasks)} tasks to {output_file}")
    print("=" * 60)


def cmd_yesterday(args):
    """Show activity from the previous work day."""
    work_day_start, work_day_end, date_str = get_work_day_range()

    print(f"Fetching activity for {date_str}...")

    user = get_user()
    user_id = user.get("id")
    username = user.get("username", "Unknown")

    teams = get_teams()
    if not teams:
        print("Error: No workspaces found")
        sys.exit(1)

    team_id = teams[0].get("id")

    print("Fetching tasks...")
    tasks = get_team_tasks(team_id, work_day_start, work_day_end)

    my_tasks = []
    for task in tasks:
        assignee_ids = [a.get("id") for a in task.get("assignees", [])]
        creator_id = task.get("creator", {}).get("id")
        if str(user_id) in [str(a) for a in assignee_ids] or str(creator_id) == str(user_id):
            my_tasks.append(task)

    print(f"Found {len(my_tasks)} tasks updated on {date_str}")

    tasks_updated = []
    comments_added = []
    comments_received = []

    for task in my_tasks:
        task_id = task.get("id")
        custom_id = task.get("custom_id", task_id)
        task_name = task.get("name", "")
        status = task.get("status", {}).get("status", "")
        date_updated = task.get("date_updated")

        tasks_updated.append({
            "ticket": custom_id,
            "title": task_name,
            "status": status,
            "updated_at": format_timestamp_readable(date_updated),
            "url": f"https://app.clickup.com/t/{team_id}/{custom_id}",
        })

        try:
            comments = get_task_comments(task_id)
            for comment in comments:
                comment_date = int(comment.get("date", 0))
                commenter_id = comment.get("user", {}).get("id")
                commenter_name = comment.get("user", {}).get("username", "Unknown")

                if work_day_start <= comment_date <= work_day_end:
                    comment_text = comment.get("comment_text", "")
                    if isinstance(comment_text, list):
                        text_parts = [c.get("text", "") for c in comment_text if isinstance(c, dict)]
                        comment_text = "".join(text_parts)

                    comment_data = {
                        "ticket": custom_id,
                        "title": task_name,
                        "comment": comment_text,
                        "commented_at": format_timestamp_readable(comment_date),
                        "url": f"https://app.clickup.com/t/{team_id}/{custom_id}",
                    }

                    if str(commenter_id) == str(user_id):
                        comments_added.append(comment_data)
                    else:
                        comment_data["commenter"] = commenter_name
                        comments_received.append(comment_data)
        except httpx.HTTPStatusError:
            pass

    output = {
        "date": date_str,
        "generated_at": datetime.now().isoformat(),
        "user": username,
        "summary": {
            "tasks_updated": len(tasks_updated),
            "comments_added": len(comments_added),
            "comments_received": len(comments_received),
        },
        "tasks_updated": tasks_updated,
        "comments_added": comments_added,
        "comments_received": comments_received,
    }

    output_file = args.output or f"activity-{date_str}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nWrote {output_file}")
    print(f"  Tasks updated: {len(tasks_updated)}")
    print(f"  Comments added: {len(comments_added)}")
    print(f"  Comments received: {len(comments_received)}")


def cmd_get(args):
    """Get ticket info."""
    ticket_number = args.ticket.upper()
    task, team_id = lookup_task(ticket_number)

    title = task.get("name", "")
    status = task.get("status", {}).get("status", "")
    custom_id = task.get("custom_id", ticket_number)
    url = f"https://app.clickup.com/t/{team_id}/{custom_id}"
    description = task.get("description", "") or task.get("text_content", "") or ""

    assignees = task.get("assignees", [])
    assignee_names = [a.get("username", "Unknown") for a in assignees]
    assignee_str = ", ".join(assignee_names) if assignee_names else "Unassigned"

    print(f"Ticket:   {custom_id}")
    print(f"Title:    {title}")
    print(f"Status:   {status.upper()}")
    print(f"Assignee: {assignee_str}")
    print(f"URL:      {url}")
    if description:
        print(f"\nDescription:\n{description}")

    task_id = task.get("id")
    comments = get_task_comments(task_id)
    if comments:
        print(f"\nComments ({len(comments)}):")
        print("-" * 40)
        for comment in comments:
            commenter = comment.get("user", {}).get("username", "Unknown")
            comment_date = format_timestamp_readable(comment.get("date"))
            comment_text = extract_comment_text(comment.get("comment_text", ""))
            print(f"[{comment_date}] {commenter}:")
            print(f"  {comment_text}")
            print()


def cmd_status(args):
    """Set ticket status."""
    ticket_number = args.ticket.upper()
    new_status = args.status.lower()

    valid_statuses = ["to do", "in progress", "review"]
    if new_status not in valid_statuses:
        print(f"Error: Invalid status '{new_status}'")
        print(f"Valid statuses: {', '.join(valid_statuses)}")
        sys.exit(1)

    task, team_id = lookup_task(ticket_number)
    task_id = task.get("id")
    current_status = task.get("status", {}).get("status", "")

    print(f"Ticket: {ticket_number}")
    print(f"Current status: {current_status.upper()}")
    print(f"Setting status: {new_status.upper()}")

    try:
        updated_task = update_task_status(task_id, new_status)
        verified_status = updated_task.get("status", {}).get("status", "")
        print(f"\nSuccess! Status is now: {verified_status.upper()}")
    except httpx.HTTPStatusError as e:
        print(f"\nError: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


def cmd_comment(args):
    """Add a comment to a ticket."""
    ticket_number = args.ticket.upper()
    comment_text = args.message

    task, _ = lookup_task(ticket_number)
    task_id = task.get("id")
    title = task.get("name", "")

    print(f"Ticket: {ticket_number}")
    print(f"Title: {title}")
    print(f"Adding comment...")

    try:
        add_comment(task_id, comment_text)
        print(f"\nComment added successfully!")
    except httpx.HTTPStatusError as e:
        print(f"\nError: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


def cmd_move_to_sprint(args):
    """Move a ticket to the current sprint."""
    ticket_number = args.ticket.upper()

    print(f"Looking up ticket {ticket_number}...")
    task, team_id = lookup_task(ticket_number)
    task_id = task.get("id")
    title = task.get("name", "")

    print(f"Finding current sprint...")
    sprint_result = get_current_sprint_list()
    if not sprint_result:
        print("Error: Could not find current sprint list")
        sys.exit(1)

    sprint_list, space_name = sprint_result
    sprint_list_id = sprint_list.get("id")
    sprint_name = sprint_list.get("name")

    print(f"\nTicket: {ticket_number}")
    print(f"Title:  {title}")
    print(f"Sprint: {sprint_name} ({space_name})")
    print(f"\nMoving ticket to sprint...")

    try:
        add_task_to_list(sprint_list_id, task_id)
        print(f"\nSuccess! {ticket_number} added to '{sprint_name}'")
    except httpx.HTTPStatusError as e:
        print(f"\nError: {e.response.status_code} - {e.response.text}")
        sys.exit(1)


# =============================================================================
# Main
# =============================================================================

def main():
    if not CLICKUP_API_KEY:
        print("Error: CLICKUP_API_KEY not found in .env")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        prog="clickup-main.py",
        description="ClickUp CLI - Manage tickets and sprints from the command line.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run clickup-main.py sprint                      Export current sprint to JSON
  uv run clickup-main.py yesterday                   Show previous work day activity
  uv run clickup-main.py get ENG-3303                Get ticket details
  uv run clickup-main.py status ENG-3303 "in progress"   Update ticket status
  uv run clickup-main.py comment ENG-3303 "Done!"    Add a comment to a ticket
  uv run clickup-main.py move-to-sprint ENG-7101     Move ticket to current sprint
        """
    )

    subparsers = parser.add_subparsers(dest="command", metavar="command")

    # sprint
    sprint_parser = subparsers.add_parser(
        "sprint",
        help="Export current sprint tickets to JSON"
    )
    sprint_parser.add_argument(
        "-o", "--output",
        default="current-sprint.json",
        help="Output file (default: current-sprint.json)"
    )
    sprint_parser.set_defaults(func=cmd_sprint)

    # yesterday
    yesterday_parser = subparsers.add_parser(
        "yesterday",
        help="Show activity from the previous work day"
    )
    yesterday_parser.add_argument(
        "-o", "--output",
        help="Output file (default: activity-YYYY-MM-DD.json)"
    )
    yesterday_parser.set_defaults(func=cmd_yesterday)

    # get
    get_parser = subparsers.add_parser(
        "get",
        help="Get ticket details"
    )
    get_parser.add_argument(
        "ticket",
        help="Ticket number (e.g., ENG-3303)"
    )
    get_parser.set_defaults(func=cmd_get)

    # status
    status_parser = subparsers.add_parser(
        "status",
        help="Set ticket status"
    )
    status_parser.add_argument(
        "ticket",
        help="Ticket number (e.g., ENG-3303)"
    )
    status_parser.add_argument(
        "status",
        help="New status: 'to do', 'in progress', or 'review'"
    )
    status_parser.set_defaults(func=cmd_status)

    # comment
    comment_parser = subparsers.add_parser(
        "comment",
        help="Add a comment to a ticket"
    )
    comment_parser.add_argument(
        "ticket",
        help="Ticket number (e.g., ENG-3303)"
    )
    comment_parser.add_argument(
        "message",
        help="Comment text"
    )
    comment_parser.set_defaults(func=cmd_comment)

    # move-to-sprint
    move_sprint_parser = subparsers.add_parser(
        "move-to-sprint",
        help="Move a ticket to the current sprint"
    )
    move_sprint_parser.add_argument(
        "ticket",
        help="Ticket number (e.g., ENG-7101)"
    )
    move_sprint_parser.set_defaults(func=cmd_move_to_sprint)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
