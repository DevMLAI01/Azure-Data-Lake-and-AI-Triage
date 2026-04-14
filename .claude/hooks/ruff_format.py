"""PostToolUse hook: auto-format Python files with ruff after Edit/Write."""
import json
import subprocess
import sys


def main():
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return

    file_path = data.get("tool_input", {}).get("file_path", "")
    if not file_path or not file_path.endswith(".py"):
        return

    try:
        subprocess.run(["ruff", "format", file_path], capture_output=True, check=False)
        subprocess.run(
            ["ruff", "check", "--fix", file_path], capture_output=True, check=False
        )
        print(f"[ruff] formatted: {file_path}")
    except FileNotFoundError:
        pass  # ruff not installed — fail silently


if __name__ == "__main__":
    main()
