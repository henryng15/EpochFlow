#!/usr/bin/env bash
# Renders docs/diagrams/*.d2 to hand-drawn-style SVGs next to the sources.
# Needs D2 (https://d2lang.com, v0.9+) and network access the first time an
# icon URL is fetched; icons are embedded, so the SVGs are self-contained.
set -euo pipefail
cd "$(dirname "$0")/../docs/diagrams"
d2=$(command -v d2 || echo "$HOME/.local/bin/d2")
for src in *.d2; do
  "$d2" --sketch --pad 24 --theme 0 "$src" "${src%.d2}.svg"
  chmod 644 "${src%.d2}.svg"  # d2 writes 0600; the SVGs are published files
done
