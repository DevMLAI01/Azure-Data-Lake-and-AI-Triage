"""PreToolUse hook: warn if required auth tokens are missing before any Bash call."""
import os
import sys

REQUIRED_TOKENS = [
    "ANTHROPIC_API_KEY",
    "AZURE_OPENAI_KEY",
    "FLY_API_TOKEN",
]


def main():
    # Drain stdin (PreToolUse passes JSON but we don't need it here)
    try:
        sys.stdin.read()
    except Exception:
        pass

    missing = [k for k in REQUIRED_TOKENS if not os.environ.get(k)]
    if missing:
        print(f"[token-check] WARNING — missing env vars: {', '.join(missing)}")


if __name__ == "__main__":
    main()
