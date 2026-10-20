# Scheduling for agents

How Max, and any agent built on or extending Max, puts work on a schedule with
Boss Scheduler. Read this before creating, changing or removing a job for a
person.

The short version: never schedule from inside a chat session. Session-scoped
timers die when the session closes (and most expire after a few days). Put the
job in Boss Scheduler, which survives restarts, sleep and app updates.

## 1. Find the scheduler

```bash
BOSS_SCHED="$(command -v boss-sched || echo ~/.boss/scheduler/bin/boss-sched)"
"$BOSS_SCHED" status
```

- Exit 0: running. The output shows pid, version and job count.
- Exit 3: installed but not running. Say so plainly; BOSS starts it, or the
  person can run `launchctl kickstart gui/$(id -u)/ai.rever.boss.scheduler`.
- Binary missing: Boss Scheduler is not installed on this machine. Say that,
  and fall back to telling the person you cannot keep a durable schedule here.
  Do not improvise one with `crontab`, `at` or a hand-written LaunchAgent.

Use `--json` on every command you parse. Human output may change; JSON fields
will not change without a version bump.

## 2. Before you add anything

1. **List first.** `boss-sched --json list` and look for a job that already
   does this. Update it (remove and re-add with the same `id`) instead of
   creating a duplicate.
2. **Confirm in plain words, not cron.** Restate the job as one sentence and
   get a yes: "Every weekday at 8:00 am Pacific, I read your calendar and
   unread mail and send you a short digest here. I never send, reply to or
   delete anything." Confirm the sentence, then build the cron from it.
3. **Always set the timezone** to the person's IANA zone
   (`America/Los_Angeles`), never a bare UTC offset. The scheduler rejects
   cron jobs without one.

## 3. Naming and ownership

There is no owner field. Ownership lives in the `id`:

```
<agent>-<slug>        e.g.  max-morning-digest, max-weekly-report
```

- `<agent>` is the lowercase agent name that created it (`max`, or your
  extension's name).
- Only touch jobs with your own prefix, unless the person explicitly asks you
  to change another agent's job.
- `name` is the human label shown in `list`. Write it for the person, not for
  you.

## 4. Where the job's files live

Keep everything a job needs in one folder:

```
~/.boss/scheduler/jobs/<id>/
    prompt.md      the exact instructions the run follows
    run.sh         optional wrapper (see section 5)
    out/           anything the run writes (briefs, reports)
```

The prompt is a file, not an argv string, so it can be read, diffed and fixed
without re-adding the job. Write the prompt so a fresh agent with no chat
history can follow it: what to do, which files and accounts, what done looks
like, and the hard limits (section 7).

## 5. Agent-turn jobs

A scheduled run is a separate process with no memory of the conversation that
created it. To run an agent, the job's `command` starts the agent's headless
CLI with the prompt file. Keep the invocation in a small wrapper so the job
spec stays stable while the invocation evolves:

```bash
#!/bin/sh
# ~/.boss/scheduler/jobs/max-morning-digest/run.sh
cd "$(dirname "$0")" || exit 1
exec claude -p "$(cat prompt.md)"
```

```bash
chmod +x ~/.boss/scheduler/jobs/max-morning-digest/run.sh
boss-sched --json add --test-run \
  --id max-morning-digest \
  --name "Weekday morning digest" \
  --cron "0 8 * * 1-5" --tz America/Los_Angeles \
  --timeout 900 --max-lateness 1800 \
  --on-failure '["/bin/sh","-c","echo \"$BOSS_SCHED_JOB_NAME failed: $BOSS_SCHED_REASON\""]' \
  -- ~/.boss/scheduler/jobs/max-morning-digest/run.sh
```

The exact headless invocation (which CLI, permission flags, how it reaches the
BOSS browser) belongs to the agent's own docs. The scheduler does not care: it
runs the argv and records the exit code.

Each run receives `BOSS_SCHED_JOB_ID`, `BOSS_SCHED_RUN_ID` and
`BOSS_SCHED_SLOT` (the scheduled instant) in its environment. Use the run id
in filenames so reruns never overwrite each other.

## 6. Delivery is the job's job

The scheduler captures the last 16 KiB of stdout and stderr into history, and
that is all it does with output. It does not send messages. If the person
should hear about the result, the run itself must deliver it: post to the chat
it came from, write the brief file and notify, and so on.

Make the run's stdout a short summary of what it did, because that is what
`history` shows when someone asks "did it run?".

## 7. Hard limits for unattended runs

A scheduled run acts with nobody watching. Write these into every prompt that
could touch them:

- **No money, no commitments, nothing irreversible** (buying, paying, trading,
  sending in the person's name, deleting) unless the person approved that
  exact action, amount and date in advance. A standing "brief me" is not
  approval to act. When in doubt the run proposes and waits for a yes.
- **No secrets in prompts, job specs or output.** `state.json` and history are
  plain files. Runs fetch credentials through the agent's own vault at run time.
- **Stay inside the timeout.** Pick `timeout_seconds` for the worst normal run.
  On timeout the whole process group is killed.
- **Late is sometimes worse than never.** Set `max_lateness_seconds` so a
  laptop that wakes after the useful window skips the day instead of running
  late (the morning digest uses 1800: after 8:30 it skips).

## 8. After adding

- Always add with `--test-run`. It runs once immediately and exits non-zero if
  the run failed. Show the person the real output, not a description of it.
- If the test fails, fix it or `remove` the job and say why. Do not leave a
  broken job enabled and quiet.
- Tell the person the next run time from `list` (`state.next_run_at`, UTC;
  convert to their zone when you say it).

## 9. Answering "what's scheduled?" and "did it run?"

```bash
boss-sched --json list                    # jobs, next run, last status, failures
boss-sched --json history <id> --limit 5  # recent runs with exit code and output
```

Read these live every time. Never answer from memory or from what you set up
earlier; jobs can be disabled, removed or failing since then.

Statuses: `success`, `failed`, `timeout`, `spawn_error`, `skipped` (too late
to catch up), `interrupted` (scheduler restarted or updated mid-run).

## 10. Changing, pausing, removing

```bash
boss-sched disable <id>    # pause; keeps history; resumes from the next future slot
boss-sched enable <id>     # resume; resets the failure counter
boss-sched remove <id>     # delete the job (history is dropped with it)
boss-sched run-now <id> --wait   # run once now; does not move the schedule
```

To change a schedule or command: `remove`, then `add` with the same `id`.
Removing is the person's call. Confirm before you remove a job they did not
just ask you to remove.

## 11. When a job keeps failing

After 2 consecutive failures the job's `on_failure` command runs. After
`max_consecutive_failures` (default 5) the scheduler disables the job and
records `disabled_reason`. If you see a disabled job in `list`, tell the
person what failed (from `history`) and what you would change. Do not just
re-enable it.

## 12. What not to do

- Do not schedule with session timers, `crontab`, `at`, `launchd` plists or
  `sleep` loops. Boss Scheduler is the one place schedules live, so "what's
  scheduled?" has one answer.
- Do not edit `state.json` by hand. Use the CLI or the socket.
- Do not put a shell one-liner with secrets or long logic in `command`; use a
  wrapper script in the job folder.
- Do not create a job "disabled until approved". Get the yes first, then add
  it enabled. A disabled job is invisible to every safeguard.

## Reference

- Full CLI, job fields, catch-up, DST and restart semantics: [README](../README.md).
- Socket protocol (for agents that prefer it to the CLI): README, "Socket protocol".
