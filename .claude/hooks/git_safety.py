"""PreToolUse hook: print branch/status/remote before git push, commit, or merge."""
import json
import subprocess
import sys

GIT_TRIGGERS = ["git push", "git commit", "git merge"]


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def main():
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return

    command = data.get("tool_input", {}).get("command", "")
    if not any(trigger in command for trigger in GIT_TRIGGERS):
        return

    branch = run(["git", "branch", "--show-current"])
    status = run(["git", "status", "-s"])
    remote = run(["git", "remote", "get-url", "origin"])

    print(f"[git-safety] branch : {branch}")
    print(f"[git-safety] remote : {remote}")
    if status:
        print(f"[git-safety] changes:\n{status}")
    else:
        print("[git-safety] working tree clean")


if __name__ == "__main__":
    main()
