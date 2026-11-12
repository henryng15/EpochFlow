# Diagrams

## Purpose

Hand-drawn-style diagrams used by the README and `docs/architecture.md`. Each
`*.d2` source is committed next to its rendered `*.svg`.

| Source | Shows |
| --- | --- |
| `architecture.d2` | Processes, data flow, and where each guarantee is enforced |
| `job-lifecycle.d2` | Job states and transitions |
| `fencing.d2` | A frozen worker's late report rejected by generation |
| `external-effect.d2` | Intent, confirmation, `UNKNOWN`, and reconciliation |
| `roadmap.d2` | Version 1 milestones and Phase 2 |

## Prerequisites

[D2](https://d2lang.com) v0.9 or newer on `PATH` or in `~/.local/bin`. The
first render needs network access to fetch icons; they are embedded in the SVG.

## Steps

1. Edit the `.d2` source; keep labels in step with the code they describe.
2. Run `scripts/render-diagrams.sh`.
3. Commit the source and the SVG together.

## Verification

- Open the SVG in a browser; GitHub renders it the same way.
- Rendering is deterministic: rerunning the script on unchanged sources leaves
  the SVGs byte-identical.

## Troubleshooting

- **A container overlaps its children or sprawls:** set
  `vars.d2-config.layout-engine: elk` in that source (as `architecture.d2` does).
- **An icon fails to render:** the icon URL must return HTTP 200; remove it or
  pick another from <https://icons.terrastruct.com>.

## Related documents

- [Architecture](../architecture.md)
- [README](../../README.md)
