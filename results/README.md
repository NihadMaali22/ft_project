# Raw load-test results

Every number in the root `README.md` performance section comes from one of these
files. They are the unedited JSON written by `npm run loadtest`.

## Runs backing the reported results

These four are the final runs, each starting from the same freshly-seeded
1,000,000-row / 30-day baseline, after waiting for the database to go quiet and a
10-second unmeasured warm-up.

| File | Scenario | Headline |
| ---- | -------- | -------- |
| `results-2026-08-16T14-57-08-899Z.json` | Load | 14,994 logs/s (99.96% of target), 0 errors, 0 shed |
| `results-2026-08-16T15-01-22-722Z.json` | Stress | 15k→14,969 / 22.5k→21,745 / 30k→27,521 |
| `results-2026-08-16T15-04-50-322Z.json` | Spike | 7.5k→7,484 / 30k→28,893 (0 shed) / 7.5k→7,490 |
| `results-2026-08-16T15-08-38-331Z.json` | Breakpoint | 15k / 22.5k / 30k / 45k→44,939, all 0 errors |

## Superseded runs, kept deliberately

The five earlier `load` runs are retained because the root README cites them as
findings, not because they measure the system correctly. Each was invalidated by a
measurement-methodology bug that the README documents under *Bottlenecks discovered
and optimisations applied*:

| File | Reported | Why it is wrong |
| ---- | -------- | --------------- |
| `...T14-22-53-333Z.json` | 6,026 logs/s | Benchmarked straight through post-bulk-load autovacuum and checkpoint recovery (README item 7) |
| `...T14-27-39-738Z.json` | 696 logs/s | Same, compounded by the uncapped query probe below |
| `...T14-36-27-203Z.json` | 11,476 logs/s | Query probe fired 4 queries/s on fixed timers with no cap on outstanding requests (README item 8) |
| `...T14-42-51-490Z.json` | 5,744 logs/s | Same probe defect, plus time predicates that defeated partition pruning (README item 1) |
| `...T14-50-04-722Z.json` | 14,995 logs/s | Ingestion correct; aggregation latency still inflated by allowing 3 concurrent copies of the same probe query |

The fixes were to the *harness and the query layer*, not to the ingestion path, which
is why the 14-50 run already shows correct throughput. Keeping these files makes the
progression auditable rather than asking the reader to take the final numbers on
trust.

## Reproducing

```bash
docker compose up -d
npm run seed                              # 1,000,000 rows across 30 days
npm run loadtest                          # all four scenarios
npm run loadtest -- --scenario=breakpoint # one scenario
```

Each file contains, per phase: target and achieved throughput, requests sent /
completed / shed / failed, latency percentiles measured from scheduled send time
(coordinated-omission corrected) and from the wire, the status-code histogram,
container CPU and memory samples, query-probe latencies, and a reconciliation of rows
actually stored against rows the service reported accepting.
