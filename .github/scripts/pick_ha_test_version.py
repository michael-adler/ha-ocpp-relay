#!/usr/bin/env python3
"""Print the newest pytest-homeassistant-custom-component release that pins a
stable (non-beta/rc/dev) homeassistant core version.

pytest-homeassistant-custom-component's own version numbers don't indicate
whether the homeassistant core they pin is a beta, so pip's resolver can't
tell the two apart on its own -- it just installs the newest release, which
during Home Assistant's ~2-week monthly beta window pins a beta core. This
script inspects each release's metadata directly and picks the newest one
that pins a final release.
"""

import json
import re
import sys
import urllib.request

PACKAGE = "pytest-homeassistant-custom-component"
INDEX = f"https://pypi.org/pypi/{PACKAGE}/json"
VERSION_RE = re.compile(r"^\d+(\.\d+)+$")
HA_PIN_RE = re.compile(r"^homeassistant\s*==\s*([^\s;]+)")


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def is_stable(version):
    return bool(VERSION_RE.match(version))


def ha_pin(requires_dist):
    for req in requires_dist or []:
        match = HA_PIN_RE.match(req.strip())
        if match:
            return match.group(1)
    return None


def main():
    index = fetch_json(INDEX)
    candidates = [
        v
        for v, files in index["releases"].items()
        if VERSION_RE.match(v) and files and not all(f.get("yanked") for f in files)
    ]
    candidates.sort(key=lambda v: [int(p) for p in v.split(".")], reverse=True)

    for version in candidates:
        info = fetch_json(f"https://pypi.org/pypi/{PACKAGE}/{version}/json")["info"]
        pinned_ha = ha_pin(info.get("requires_dist"))
        if pinned_ha and is_stable(pinned_ha):
            print(version)
            return

    print(f"no {PACKAGE} release pinning a stable homeassistant found", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
