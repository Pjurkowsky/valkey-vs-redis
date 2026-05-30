# Agent Instructions

This repository contains benchmark tooling for comparing:

- self-hosted Valkey Cluster in Kubernetes,
- self-hosted Redis 7.2 Cluster in Kubernetes,
- Google Cloud Memorystore for Redis Cluster.

When writing or changing benchmark code, prefer preserving the existing script style and result format over introducing a new framework.

## Benchmark Scripts

- Put Kubernetes and cloud benchmark scripts in `k8s/scripts/`.
- Use Bash with `set -euo pipefail`.
- Keep scripts runnable from the repository root.
- Support repeated runs through `N`.
- Print the effective configuration at startup.
- Do not hardcode Kubernetes namespaces, release names, cluster names, regions, project IDs, endpoints, or image names.
- Expose runtime configuration through environment variables such as `NS`, `RELEASE`, `PROVIDER`, `HOST`, `PORT`, `PROJECT_ID`, `LOCATION`, `VALUES_FILE`, `CHART_PATH`, `MEMTIER_IMAGE`, and `BACKUP_IMAGE`.
- Use provider selection explicitly when a benchmark can target more than one system, for example `PROVIDER=valkey|redis|memorystore`.
- For self-hosted Kubernetes variants, keep Valkey and Redis 7.2 benchmark logic as symmetrical as possible.
- For Memorystore, treat managed operations as black-box cloud operations and record the visible operation duration and ready-state wait duration.

## Kubernetes Execution

- Run benchmark clients as Kubernetes pods unless the existing benchmark explicitly uses a local command.
- Use `k8s/scripts/pod_results.sh` for long-running command pods.
- Long-running pods should write their exit code to `$POD_EXIT_CODE_FILE`, touch `$POD_DONE_FILE`, and remain alive for `$POD_HOLD_SECONDS` so logs and generated files can be copied.
- Always copy raw pod reports back to the local `results/` tree.
- Use `kubectl wait`, `kubectl rollout status`, or explicit readiness loops before measuring workload-dependent phases.
- Avoid relying only on pod phase if the benchmark needs application-level readiness; check cluster health, slot coverage, or endpoint readiness as appropriate.
- Do not assume a fixed namespace like `vk`; scripts must work for separate workspaces used for Valkey, Redis 7.2, or other runs.

## Data Generation

- Use `k8s/images/backup/backup_restore_seed.py` for seed, verify, cleanup, snapshot, flush, and configuration helper operations when possible.
- For backup/restore benchmarks, prefer deterministic random data with `--random-data` so RDB or snapshot sizes reflect realistic data and are not dominated by compression artifacts.
- Seed reports must include enough metadata to verify data after restore: `run_id`, key count, data size, random-data flag, write errors, and completion status.
- Verification must use the seed report from the same run.
- If partial writes are allowed for pressure tests, record this explicitly in the report.

## Timing

- Use epoch seconds for coarse phase timestamps and seconds for durations.
- Use the `_duration_s` suffix for duration fields.
- Split timings into meaningful phases rather than a single total when the benchmark is operational:
  - seed duration,
  - backup or snapshot creation duration,
  - copy or upload duration,
  - validation duration,
  - managed operation duration,
  - pod recreation duration,
  - cluster recovery after pods are ready,
  - restore duration,
  - verification duration.
- For availability benchmarks, record both workload-side symptoms and infrastructure events. Examples: ops/sec drop, timeouts, reconnection events, pod restarts, failover duration, and recovery.

## Backup And Restore

- Validate backups before using them for restore whenever the toolchain supports it.
- For Valkey and Redis RDB files, use `valkey-check-rdb` or `redis-check-rdb` when available.
- Treat `kubectl cp` as unreliable for large files unless the script verifies copied file size and retries failed copies.
- Restore benchmarks may be destructive, but this must be clear from the script name, printed startup config, and comments.
- If a restore requires a fresh cluster, delete and recreate only resources owned by the configured release/workspace.
- Do not silently reuse data from previous runs.

## Results

- Store results under `./results/<scenario>/` by default.
- Write one JSON report per run.
- Include a stable `variant` field in every report.
- Include the provider, benchmarked system, run number, input size, workload parameters, image tags, chart values file, and important raw artifact paths.
- If the benchmark has multiple runs, also write a machine-readable summary such as CSV.
- Avoid overwriting previous run directories unless the user explicitly asks for cleanup.
- Keep raw logs, manifests, timing JSON, seed reports, verify reports, and backup manifests when they are needed to audit a result.

## Analysis Code

- Add or update `cli.py` analysis commands when a new result type needs repeatable reporting or plots.
- Analysis should read raw result files from `./results/...` and write plots or summaries to `./plots/...`.
- Prefer robust parsing of JSON/CSV outputs over parsing terminal text.
- When computing averages, report the number of analysed runs and whether integrity checks passed.

## Comparability

- Clearly distinguish the three benchmarked systems:
  - Valkey Cluster in Kubernetes,
  - Redis 7.2 Cluster in Kubernetes,
  - Memorystore for Redis Cluster.
- For Valkey vs Redis 7.2, keep topology, resource limits, data generation, client image, workload shape, and run count aligned unless the scenario intentionally varies them.
- For Memorystore, record the managed configuration that is visible to the user: node type, shard count, replica count, endpoint, region, network, persistence/backup settings, and Redis configs.
- Do not claim that self-hosted Kubernetes and Memorystore are identical environments. Treat Memorystore as a managed baseline with less operational control.

## Safety

- Never run destructive Kubernetes or cloud cleanup automatically unless the script name and documentation make the destructive behavior clear.
- Prefer scoped selectors based on configured release labels instead of broad namespace deletes.
- Print enough context before destructive phases so a human can see what is about to be affected.
- Do not commit generated benchmark results, plots, kubeconfigs, credentials, or cloud-specific secrets unless the user explicitly asks.
