"""Print the make help message by parsing the Makefile.

Used by the `help` target. Works the same in cmd.exe, PowerShell, Git Bash,
and Linux/macOS shells — unlike the awk-based equivalent.

Conventions read from the Makefile:
- `## description`  on the line above a target  -> target's help text
- `##@ Section name`                            -> section header
"""
import re
import sys

_SECTION = re.compile(r"^##@ (.+)$")
_DESC = re.compile(r"^## (.+)$")
_TARGET = re.compile(r"^([a-zA-Z][\w-]*):")


def main(makefile_path: str = "Makefile") -> None:
    print()
    print("Usage: make <target>")
    desc: str | None = None
    with open(makefile_path, encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if m := _SECTION.match(line):
                print()
                print(m.group(1))
                continue
            if m := _DESC.match(line):
                desc = m.group(1)
                continue
            if desc and (m := _TARGET.match(line)):
                print(f"  {m.group(1):<18} {desc}")
                desc = None
    print()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "Makefile")
