# AGENTS.md

Two kinds of agent read this repo:

- **Agents that schedule work for a person** (Max and agents extending it):
  read [docs/scheduling-for-agents.md](docs/scheduling-for-agents.md). It covers
  finding the scheduler, naming, prompts, delivery, and the limits for
  unattended runs.
- **Agents changing the scheduler itself**: read [README.md](README.md), then
  keep the one-job rule. Boss Scheduler decides when jobs are due, runs their
  argv and records outcomes. No terminals, MCP, UI, tray or BOSS code. Anything
  else belongs in the job or in BOSS.

Build and test:

```bash
cargo build --release
cargo test
```

Prose uses spaced hyphens, never em-dashes.
