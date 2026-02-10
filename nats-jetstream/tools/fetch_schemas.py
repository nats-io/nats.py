#!/usr/bin/env python3
"""Script to fetch and maintain JetStream API schemas from jsm.go."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd: list[str], cwd: str | None = None) -> None:
    """Run a command and exit on failure.

    Args:
        cmd: Command and arguments to run
        cwd: Optional working directory
    """
    try:
        subprocess.run(cmd, check=True, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}: {' '.join(cmd)}", file=sys.stderr)
        sys.exit(1)


def fix_json_file(path: Path) -> None:
    """Fix JSON file by removing trailing commas and formatting.

    Args:
        path: Path to JSON file to fix
    """
    try:
        # Read file content
        content = path.read_text()

        # Remove trailing commas in objects
        content = re.sub(r",(\s*})", r"\1", content)
        # Remove trailing commas in arrays
        content = re.sub(r",(\s*])", r"\1", content)

        # Parse and format JSON
        data = json.loads(content)
        path.write_text(json.dumps(data, indent=2) + "\n")
    except Exception as e:
        print(f"Error fixing JSON file {path}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Main entry point."""
    # Get the root directory of our project
    root_dir = Path(__file__).parent.parent

    # Create a temporary directory for cloning
    tmp_dir = root_dir / "tmp"
    tmp_dir.mkdir(exist_ok=True)

    try:
        # Clone jsm.go repository
        jsm_dir = tmp_dir / "jsm.go"
        if jsm_dir.exists():
            print("Removing existing jsm.go clone...")
            shutil.rmtree(jsm_dir)

        print("Cloning jsm.go repository...")
        run_cmd(["git", "clone", "https://github.com/nats-io/jsm.go.git", str(jsm_dir)])

        # Create schema directory if it doesn't exist
        schema_dir = root_dir / "schemas" / "jetstream" / "api" / "v1"
        schema_dir.mkdir(parents=True, exist_ok=True)

        # Copy and fix JetStream schema files
        source_schema_dir = jsm_dir / "schema_source" / "jetstream" / "api" / "v1"
        if not source_schema_dir.exists():
            print(f"Schema directory not found: {source_schema_dir}", file=sys.stderr)
            sys.exit(1)

        print(f"Copying and fixing schema files to {schema_dir}...")
        for file in source_schema_dir.glob("*.json"):
            dest_file = schema_dir / file.name
            shutil.copy2(file, dest_file)
            fix_json_file(dest_file)
            print(f"Copied and fixed {file.name}")

    finally:
        # Cleanup
        if tmp_dir.exists():
            print("Cleaning up temporary files...")
            shutil.rmtree(tmp_dir)

    print("\nDone! Schema files are now in schemas/jetstream/api/v1/")


if __name__ == "__main__":
    main()
