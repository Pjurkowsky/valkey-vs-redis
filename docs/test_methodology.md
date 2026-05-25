# Specyfikacja metodologii badan: Valkey vs Redis na Kubernetes

Dokument opisuje, jak powinny zostac przeprowadzone poszczegolne testy oraz co stanowi wynik poddawany analizie statystycznej i wizualizacji. Opracowano na podstawie audytu istniejacych skryptow i konfiguracji w repozytorium.

---

## 0. Srodowisko testowe

### Topologia klastra Valkey / Redis

| Parametr | Wartosc |
|---|---|
| Liczba shardow | 3 |
| Repliki na shard | 1 (razem 6 podow) |
| Namespace K8s | `vk` |
| Helm chart | `../valkey-helm/valkey` z overlay `k8s/manifests/values.yaml` |
| maxmemory per pod | 1 GB (allkeys-lru) |

Instalacja:

```bash
helm upgrade --install valkey ../valkey-helm/valkey \
  -n vk --create-namespace \
  -f k8s/manifests/values.yaml
```

### Monitoring

- **Prometheus + Grafana**: `kube-prometheus-stack` w namespace `monitoring`
- **StatsD exporter**: `k8s/manifests/statsd-exporter.yaml` (konwertuje metryki memtier na format Prometheus)
- **Grafana dashboard**: import ID 763 (Redis/Valkey overview)

### Klient testowy

Wszystkie testy sa uruchamiane z poda wewnatrz tego samego klastra K8s, aby wyeliminowac dodatkowe opoznienia sieciowe (load balancery, VPC peering, Internet).

```
Namespace: vk
Service Account: memtier-sa (k8s/memtier_rbac.yaml)
Image: memtier_k8s:1  (k8s/images/memtier/Dockerfile)
```

### Wymagania do porownania hipotez H1, H2, H3

Zeby zweryfikowac hipotezy, potrzebne sa **trzy** srodowiska uruchamiane na tym samym klastrze K8s:

| System | Cel | Helm chart |
|---|---|---|
| Valkey Cluster self-hosted | glowny przedmiot badan | `../valkey-helm/valkey` |
| Redis Cluster self-hosted | punkt odniesienia H1, H3 | np. Bitnami `redis-cluster` (identyczne zasoby CPU/RAM) |
| Memorystore (GCP managed Redis) | punkt odniesienia H2, H3 | brak (managed service, endpoint z VPC) |

> **Kluczowe**: konfiguracje CPU/RAM dla Valkey i Redis Cluster musza byc identyczne, aby porownanie bylo miarodajne.

---

## 1. Benchmark wydajnosci (H1)

### Hipoteza

H1: Klaster Valkey osiaga wyzszy throughput (OPS) i nizsze latencje (p95/p99/p99.9) niz Redis Cluster przy tym samym alokowanym zasobie CPU.

### Zmienne niezalezne

| Zmienna | Wartosci |
|---|---|
| Liczba vCPU (limit poda Valkey) | 1, 2, 4 |
| Rozmiar payloadu | 1 KB, 10 KB, 1000 KB (1 MB) |
| Rodzaj operacji (ratio reads:writes) | `0:1` (write-only), `1:0` (read-only), `1:1` (mixed) |

### Parametry memtier_benchmark

```
--cluster-mode
--threads 4
--clients 25
--pipeline 10
--test-time 300
--key-maximum 1000000
--run-count 5
--print-all-runs
--print-percentiles 50,95,99,99.9
--json-out-file <cpu>_<payload>_<ratio>.json
```

> **Uwaga**: `--pipeline 10` moze zawyzyc throughput w stosunku do scenariuszy produkcyjnych. W pracy nalezy jawnie raportowac ten parametr jako czesc konfiguracji testu.

### Procedura

1. Zainstaluj Valkey i Redis Cluster z identycznymi Helm values (ta sama liczba shardow, ta sama wielkosc maxmemory).
2. Zbuduj i wgraj obraz `memtier_k8s:1` do klastra.
3. Uruchom RBAC: `kubectl apply -f k8s/memtier_rbac.yaml`.
4. **(Warm-up)** Przed wlasciwym testem uruchom 30-sekundowy warm-up na tym samym keyspace, zeby zaladowac dane do pamieci Valkey:

   ```bash
   memtier_benchmark --cluster-mode -s valkey.vk.svc.cluster.local -p 6379 \
     --threads 4 --clients 25 --test-time 30 --key-maximum 1000000
   ```

5. Uruchom benchmark przez `k8s/scripts/run_benchmark.sh` (skrypt automatycznie zmienia CPU limit przed kazdym zestawem konfiguracji):

   ```bash
   bash k8s/scripts/run_benchmark.sh --output ./results/valkey_memtier
   ```

6. Powtorz identycznie dla Redis Cluster (z innym hostem/namespacem).

### Metryki zbierane

| Metryka | Zrodlo |
|---|---|
| ops/sec | memtier JSON: `RUN #N RESULTS > Totals > Ops/sec` |
| p50, p95, p99, p99.9 latency (ms) | memtier JSON: `Totals > Percentile Latencies` |
| CPU utilization (cores) | Prometheus: `rate(container_cpu_usage_seconds_total{pod=~"valkey-.*"}[30s])` |
| Memory (working set, MB) | Prometheus: `container_memory_working_set_bytes{pod=~"valkey-.*"}` |

### Wynik do analizy

Plik `summary.csv` generowany przez:

```bash
python cli.py benchmark --input ./results/valkey_memtier --output-dir ./plots/valkey_benchmark
python cli.py benchmark --input ./results/redis_memtier  --output-dir ./plots/redis_benchmark
```

**Schemat CSV**: `cpu, payload_kb, ratio, run_id, ops_sec, p50, p95, p99, p999, cpu_util_cores, mem_mb`

**Analiza statystyczna**:
- Dla kazdej kombinacji `(payload, ratio)` oblicz mean ± std ops/sec i latencji per system (Valkey / Redis) na N=5 powtorzeniach
- Test Manna-Whitneya U (lub t-Studenta jezeli rozklady sa normalne) dla porownan Valkey vs Redis
- Efekt: odrzucenie H0 (brak roznicy) na poziomie alfa=0.05 oznacza istotna statystycznie roznice

**Wykresy**:
- `ops_sec_{ratio}_{payload}kb.png`: grupowane slupki mean ± std per vCPU, dla Valkey i Redis obok siebie
- `latency_{ratio}_{payload}kb.png`: p50/p95/p99/p99.9 per vCPU
- `heatmap_{ratio}.png`: heatmapa ops/sec (payload x vCPU)
- `cpu_util_*.png`, `mem_usage_*.png`: zuzycie zasobow

---

## 2. Failover Time (H3)

### Hipoteza

H3: Czas odtworzenia mastera po awarii (failover time) jest krotszy w rozwiazaniach z natywnymi operatorami Kubernetes niz w Redis Sentinel i porownywalny z mechanizmami Memorystore.

### Procedura

1. Uruchom memtier z niskim concurrency (1 client, 4 watki) na 120 sekund w trybie mieszanym `1:1`.
2. Odczekaj 30 sekund steady state.
3. Wykryj aktualnego mastera: `kubectl exec valkey-0 -n vk -- valkey-cli cluster nodes | grep master`.
4. Wstrzyknij PodChaos (kill mastera, `gracePeriod: 1`).
5. Memtier kontynuuje -- zapisuj timestampowane logi do pliku `failover_run_N.log`.
6. Po zakonczeniu testu (120s) skopiuj JSON i log.

Uruchomienie:

```bash
N=5 bash k8s/scripts/failover_benchmark.sh ./results/valkey_failover
```

> **Wazne**: uruchamiac z `N=5` (domyslnie N=1). Dla wiarygodnych sredniej i odchylenia standardowego konieczne sa co najmniej 5 powtorzen.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `failover_duration_ms` | Czas (ms) przez ktory ops/sec spada ponizej 50% basline'u | `failover_summary.csv` |
| `ops_lost` | Szacunkowa liczba straconych operacji w oknie failoveru | `failover_summary.csv` |
| `clusterdown_errors` | Liczba odpowiedzi CLUSTERDOWN od Valkey podczas failoveru | `failover_run_N.log` |
| `peak_p99_during` | Maksymalny p99 latency podczas failoveru | `failover_summary.csv` |
| `baseline_ops` | Srednie ops/sec przed failoverem | `failover_summary.csv` |

> **Ograniczenie rozdzielczosci**: memtier raportuje buckets per sekunde. Wartosc `failover_duration_ms` jest wiec zaokraglona do wielokrotnosci 1000 ms. Dokladniejszy pomiar jest mozliwy z timestampow w pliku `.log`.

### Wynik do analizy

```bash
python cli.py failover --input ./results/valkey_failover --output-dir ./plots/valkey_failover
```

**Schemat CSV**: `file, failover_detected, failover_duration_ms, ops_lost, baseline_ops, peak_p99_during, clusterdown_errors, chaos_second, ...`

**Analiza statystyczna**:
- Srednia i odchylenie standardowe `failover_duration_ms` na N=5 powtorzeniach per system
- Porownanie: Valkey K8s operator vs Redis Sentinel vs Memorystore (jezeli dostepny)
- Test Manna-Whitneya U miedzy systemami

**Wykresy**:
- `failover_run_N.png`: 3 panele -- ops/sec (z zaznaczonym oknem failoveru), p99/p50 latency, CLUSTERDOWN errors/s
- `failover_comparison.png`: slupki failover duration per run z linia mean

---

## 3. Data Consistency -- Split-Brain

### Cel

Zbadac, czy podczas split-brain (network partition miedzy wezlami Valkey) dochodzi do utraty zaakceptowanych zapisow (acknowledged writes). Odpowiedz na pytanie: ile kluczy z ACK'd `SET` na minority-side slots jest traconych po naprawie partycji i promowaniu nowego mastera przez majority?

### Mechanizm split-brain w Valkey/Redis Cluster

Valkey Cluster nie gwarantuje strong consistency. Podczas split-brain:
1. Minority master jednego shardu jest odciety od majority, a jego replika pozostaje po stronie majority.
2. Minority master moze przez krotki czas (do `cluster-node-timeout`) nadal przyjmowac zapisy i zwracac ACK.
3. Majority wykrywa niedostepnosc minority mastera i promuje jego replike na nowego mastera.
4. Po naprawie partycji minority master wraca jako replika -- jego dane sa nadpisywane przez nowego mastera z majority.
5. Klucze ACK'd przez minority mastera w oknie miedzy partycja a timeout zostaja utracone.

### Procedura

Skrypt: `k8s/scripts/split_brain_benchmark.sh`
Checker: `k8s/images/consistency/split_brain_check.py`
Chaos: `k8s/chaos/split-brain-partition.yaml`

```bash
N=5 bash k8s/scripts/split_brain_benchmark.sh ./results/valkey_split_brain
```

**Sekwencja (per run)**:
1. Skrypt odkrywa topologie klastra (`valkey-cli cluster nodes`) i wybiera master jednego shardu jako minority; jego replika zostaje po stronie majority.
2. Labeluje pody: `chaos-side=minority` (1 pod) i `chaos-side=majority` (pozostale pody, w tym replika minority mastera).
3. Uruchamia `split_brain_check.py` -- klient pisze klucze **bez hash tagow** (format `sb.RUN.cID.SEQ`) aby klucze byly rozproszone po wszystkich slotach/shardach.
4. Checker rejestruje, ktory klucz trafia do slotu minority, a ktory do majority (na podstawie CRC16 i CLUSTER SLOTS).
5. Po 30s steady state, skrypt aplikuje `split-brain-partition.yaml` -- obustronny network partition miedzy minority a majority (90s).
6. Klient kontynuuje pisanie. Klucze kierowane do minority mastera moga byc ACK'd w oknie miedzy partycja a `cluster-node-timeout`. Po tym czasie minority master zwraca CLUSTERDOWN, a majority promuje replike.
7. Partycja jest usuwana (heal). Minority master wraca jako replika majority.
8. Po 15s stabilizacji, checker weryfikuje **wszystkie** ACK'd klucze: klucze z minority slots moga byc utracone.
9. Raport JSON zawiera podzial: `keys_missing_minority` vs `keys_missing_majority`.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `keys_missing` | Laczna liczba ACK'd kluczy utraconych po naprawie partycji | `split_brain_run_N.json` |
| `keys_missing_minority` | Klucze utracone z minority-side slots (oczekiwane > 0) | `split_brain_run_N.json` |
| `keys_missing_majority` | Klucze utracone z majority-side slots (oczekiwane = 0) | `split_brain_run_N.json` |
| `loss_rate` | `keys_missing / total_acked` | `split_brain_run_N.json` |
| `minority_loss_rate` | `keys_missing_minority / acked_minority` | `split_brain_run_N.json` |
| `majority_loss_rate` | `keys_missing_majority / acked_majority` | `split_brain_run_N.json` |
| `total_failed` | Liczba requestow z bledem (ClusterDown, Timeout) | `split_brain_run_N.json` |
| `affected_rate` | `(failed + slow) / total_attempted` | `split_brain_run_N.json` |
| `acked_minority` / `acked_majority` | Liczba ACK'd kluczy per strona partycji | `split_brain_run_N.json` |
| `write_rate_per_second` | Szereg czasowy z podzilem na minority/majority | `split_brain_run_N.json` |

### Wynik do analizy

```bash
python cli.py split-brain --input ./results/valkey_split_brain --output-dir ./plots/valkey_split_brain
```

**Schemat CSV**: `file, run_id, minority_pods, total_acked, acked_minority, acked_majority, keys_missing, keys_missing_minority, keys_missing_majority, loss_rate, minority_loss_rate, majority_loss_rate, ...`

**Analiza statystyczna**:
- `keys_missing_minority` mean ± std -- oczekiwany wynik: > 0 (utrata danych na minority po heal)
- `keys_missing_majority` mean ± std -- oczekiwany wynik: 0 (majority nie traci danych)
- `minority_loss_rate` vs `majority_loss_rate` -- potwierdzenie asymetrii utraty danych
- `affected_rate` mean ± std -- procent requestow widocznych jako blad

**Wykresy (4-panelowe per run)**:
- `split_brain_run_N.png`:
  - Panel 1: total attempted/ACK'd writes/s z oznaczonym oknem impactu
  - Panel 2: stackplot ACK'd/s minority vs majority (pokazuje kiedy minority przestaje potwierdzac)
  - Panel 3: p95/p99 latency
  - Panel 4: failed writes/s z podzilem na minority/majority slots + % affected
- `split_brain_comparison.png`:
  - Panel 1: stacked bar keys_missing (minority + majority) per run
  - Panel 2: loss rate minority vs majority (grouped bars)
  - Panel 3: affected request rate per run

---

## 4. Resilience -- CPU throttling i OOM Kill (H4)

### Cel

Zbadac, jak klaster Valkey zachowuje sie podczas niedostepnosci zasobow (CPU throttling, OOM Kill). Mierzy sie degradacje throughput i latencji oraz czas powrotu do normalnego dzialania.

### Scenariusze

| Scenariusz | Chaos | Parametry | Czas trwania |
|---|---|---|---|
| CPU stress | `StressChaos` | 4 workers, 100% CPU load | 30s |
| Memory stress | `StressChaos` | 900 MB, 1 worker | 60s |
| Memory extreme stress | `StressChaos` | 1800 MB, 2 workers | 60s |

> **Uwaga dotyczaca OOM Kill**: aby rzeczywiscie wywolac OOM Kill poda Valkey, konieczne jest ustawienie `resources.limits.memory` w Helm values (np. `1.5Gi`) i zwiekszenie stress do wartosci przekraczajacej limit (np. 1.2 GB). Bez jawnego memory limitu kernel OOM killer nie zostanie uruchomiony przez Chaos Mesh.

### Procedura

```bash
N=5 bash k8s/scripts/resilience_benchmark.sh cpu  ./results/valkey_resilience
N=5 bash k8s/scripts/resilience_benchmark.sh memory ./results/valkey_resilience
N=5 bash k8s/scripts/resilience_benchmark.sh memory-extreme ./results/valkey_resilience
```

**Sekwencja (per run)**:
1. Start memtier: 120s, 4 watki, 16 klientow, ratio 1:1, 1 KB payload.
2. Odczekaj 30s steady state.
3. Zastosuj StressChaos (`stress-cpu.yaml` lub `stress-memory.yaml`).
4. Chaos trwa 30s (CPU) lub 60s (memory).
5. Chaos konczy sie automatycznie; memtier kontynuuje do 120s.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `ops_drop_pct` | Maksymalny procentowy spadek ops/sec wzgledem basline'u | `resilience_{scenario}_summary.csv` |
| `degradation_duration_ms` | Czas (ms) przez ktory ops/sec pozostaje ponizej 80% basline'u | `resilience_{scenario}_summary.csv` |
| `peak_p99_during` | Maksymalne p99 latency podczas stresu | `resilience_{scenario}_summary.csv` |
| `recovery_detected` | Czy ops/sec wrocil do >=90% basline'u po zakonczeniu stresu | `resilience_{scenario}_summary.csv` |
| `pod_restart_detected` | Czy wykryto >=2 sekundy z ops=0 (heurystyka OOM Kill lub restart) | `resilience_{scenario}_summary.csv` |

### Wynik do analizy

```bash
python cli.py resilience --scenario cpu    --input ./results/valkey_resilience --output-dir ./plots/valkey_resilience
python cli.py resilience --scenario memory --input ./results/valkey_resilience --output-dir ./plots/valkey_resilience
python cli.py resilience --scenario memory-extreme --input ./results/valkey_resilience --output-dir ./plots/valkey_resilience
```

**Schemat CSV**: `file, scenario, degradation_detected, baseline_ops, ops_drop_pct, degradation_duration_ms, peak_p99_during, recovery_detected, pod_restart_detected, ...`

**Analiza statystyczna**:
- `ops_drop_pct` i `degradation_duration_ms` mean ± std per scenariusz
- Procent runs z `recovery_detected=True`
- Procent runs z `pod_restart_detected=True` (jako wskaznik OOM Kill)

**Wykresy**:
- `resilience_{cpu|mem}_run_N.png`: ops/sec i p99/p50 z zaznaczonym oknem degradacji i oknem stresu (zielone/czerwone pionowe linie)
- `resilience_{cpu|mem}_comparison.png`: slupki ops_drop_pct i peak_p99 per run

---

### 4a. Maxmemory -- eviction pressure

Ten test sprawdza zachowanie polityk `maxmemory-policy allkeys-lru` oraz
`maxmemory-policy volatile-lru`. Nie uzywa Chaos Mesh. Zamiast tego zapisuje
wiecej danych niz klaster moze utrzymac w pamieci i mierzy liczniki eviction,
bledy `OOM command not allowed`, liczbe zaakceptowanych zapisow oraz odsetek
brakujacych kluczy w probce weryfikacyjnej.

```bash
BACKUP_IMAGE=europe-central2-docker.pkg.dev/redis-vs-valkey/valkey-bench/backup_restore:1 \
VALKEY_MAXMEMORY=1gb \
MAXMEMORY_POLICIES="allkeys-lru volatile-lru" \
N=1 \
bash k8s/scripts/maxmemory_benchmark.sh 4096 ./results/valkey_maxmemory
```

Dla Memorystore mozna uzyc tego samego mechanizmu. Aby test byl porownywalny
z klastrem Valkey, skrypt moze ustawic `maxmemory` na 1 GiB per shard przez
konfiguracje zarzadzanej instancji:

```bash
BACKUP_IMAGE=europe-central2-docker.pkg.dev/redis-vs-valkey/valkey-bench/backup_restore:1 \
PROVIDER=memorystore \
MEMORYSTORE_CLUSTER_ID=redis-ms-2 \
MEMORYSTORE_MAXMEMORY=1073741824 \
MAXMEMORY_POLICIES="allkeys-lru volatile-lru" \
N=1 \
bash k8s/scripts/maxmemory_benchmark.sh 4096 ./results/memorystore_maxmemory
```

**Sekwencja (per run)**:
1. Ustaw badana polityke `maxmemory-policy`.
2. Wyczysc klucze testowe i zresetuj statystyki.
3. Zapisz snapshot `INFO memory stats` oraz `DBSIZE` z kazdego mastera.
4. Wpisz `TARGET_MB` danych jako klucze 1 KB rozproszone po klastrze.
   Domyslnie klucze nie maja TTL.
5. Zapisz drugi snapshot `INFO memory stats` oraz `DBSIZE`.
6. Zweryfikuj probke zaakceptowanych zapisow.
7. Zapisz `evicted_keys_delta`, `oom_errors`, `write_errors`,
   `sample_missing_rate`, `used_memory_after`, `dbsize_after`.

**Interpretacja**:
- `allkeys-lru` moze usuwac dowolne klucze, wiec zapis powinien byc kontynuowany
  po osiagnieciu limitu pamieci, kosztem eviction.
- `volatile-lru` usuwa tylko klucze z TTL. Przy domyslnym generatorze bez TTL
  oczekiwane sa bledy `OOM command not allowed`, poniewaz Redis/Valkey nie ma
  kandydatow do usuniecia.
- `KEY_TTL_SECONDS=<n>` pozwala uruchomic wariant, w ktorym zapisywane klucze sa
  kandydatami do eviction rowniez dla `volatile-lru`.
- To nie jest test OOM Kill. Do OOM Kill potrzebny jest Kubernetes
  `resources.limits.memory`.

---

## 5. Upgrade Zero-Downtime (H4)

### Cel

Zmierzyc liczbe i czas trwania przerw w dostepnosci (disruptions) widocznych dla aplikacji podczas rolling upgrade klastra Valkey pod ciaglym obciazeniem.

### Procedura

```bash
N=5 bash k8s/scripts/upgrade_benchmark.sh ./results/valkey_upgrade
```

**Sekwencja (per run)**:
1. Start memtier: 120s, 4 watki, 16 klientow, ratio 1:1.
2. Odczekaj 30s steady state.
3. Wykonaj `helm upgrade` z `podAnnotations.restart-trigger=<epoch>` -- wymusza rolling restart wszystkich podow Valkey.
4. Memtier kontynuuje przez pozostale ~240s.

> **Uwaga**: test mierzy **rolling restart** (ten sam obraz), nie rzeczywisty upgrade do nowej wersji. Aby zmierzyc prawdziwy upgrade, nalezy zmienic `image.tag` w Helm values na nowa wersje Valkey.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `disruptions_detected` | Liczba okien, w ktorych ops/sec spada ponizej 80% basline'u | `upgrade_summary.csv` |
| `total_disrupted_ms` | Laczny czas (ms) wszystkich okien disruption | `upgrade_summary.csv` |
| `max_single_disruption_ms` | Najdluzsze pojedyncze okno disruption | `upgrade_summary.csv` |
| `total_ops_lost` | Szacunkowe straty operacji we wszystkich oknach | `upgrade_summary.csv` |
| `upgrade_clean` | `True` jezeli kazde okno trwalo <= 2s | `upgrade_summary.csv` |

### Wynik do analizy

```bash
python cli.py upgrade --input ./results/valkey_upgrade --output-dir ./plots/valkey_upgrade
```

**Schemat CSV**: `file, disruptions_detected, total_disrupted_ms, max_single_disruption_ms, total_ops_lost, upgrade_clean, baseline_ops, peak_p99_during, ...`

**Analiza statystyczna**:
- `total_disrupted_ms` mean ± std na N=5 powtorzeniach
- `disruptions_detected` mean (oczekiwana: liczba podow = 6)
- Procent runs z `upgrade_clean=True`

**Wykresy**:
- `upgrade_run_N.png`: ops/sec i p99/p50 z zaznaczonymi oknami disruption (pomaranczowe span-y)
- `upgrade_comparison.png`: total_disrupted_ms i disruptions_detected per run

---

## 6. Resharding -- Horizontal Scale (H4)

### Cel

Zmierzyc czas i koszt operacyjny zmiany liczby shardow w obu kierunkach: rozszerzenia klastra z 3 do 4 shardow oraz powrotu z 4 do 3 shardow pod ciaglym obciazeniem. Zbadac wplyw migracji slotow na przepustowosc i latency widoczne dla klientow.

### Procedura

```bash
N=5 bash k8s/scripts/reshard_benchmark.sh ./results/valkey_reshard
```

**Sekwencja (per run, reshard up)**:
1. Start memtier: 120s, 4 watki, 16 klientow, ratio 1:1.
2. Odczekaj 30s steady state.
3. Sprawdz, czy klaster ma `cluster_state:ok` i czy nie ma aktywnych eksperymentow Chaos Mesh.
4. Ustaw `rollingUpdate.partition=6` na StatefulSecie, aby stare pody `valkey-0..valkey-5` nie byly restartowane podczas scale-upu.
5. `helm upgrade --set cluster.shards=4` -- dodaj nowy shard (pody `valkey-6` i `valkey-7`).
6. Czekaj na gotowosc nowych podow oraz pojawienie sie ich w `CLUSTER NODES`.
7. Nie uruchamiaj manualnego `valkey-cli --cluster rebalance`; obserwuj auto-rebalance wykonywany przez init/chart i zapisuj trace slotow na nowym masterze.
8. Memtier kontynuuje do konca (120s), wynik trafia do `reshard_run_N.json`.

**Sekwencja (per run, reshard down)**:
1. Start drugiego memtier: 120s, 4 watki, 16 klientow, ratio 1:1.
2. Odczekaj 30s steady state na klastrze 4-shardowym.
3. Przenies sloty z nowego mastera rownomiernie na oryginalne mastery.
4. Usun nowe wezly z metadanych klastra przez `valkey-cli --cluster del-node`.
5. `helm upgrade --set cluster.shards=3` -- usun pody `valkey-6` i `valkey-7`.
6. Memtier kontynuuje do konca (120s), wynik trafia do `reshard_down_run_N.json`.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `operation_duration_s` | Czas calej operacji reshardingu: scale-up + auto-rebalance dla fazy `up`, albo move-slots + del-node + scale-down dla fazy `down` | `reshard_summary.csv` |
| `scale_duration_s` | Czas scale-upu dla fazy `up` | `reshard_summary.csv` |
| `rebalance_duration_s` | Czas wykrytego auto-rebalance dla fazy `up` | `reshard_summary.csv` |
| `reshard_down_duration_s` | Czas przenoszenia slotow z usuwanego sharda dla fazy `down` | `reshard_summary.csv` |
| `del_node_duration_s` | Czas usuwania dodatkowych wezlow z metadanych klastra dla fazy `down` | `reshard_summary.csv` |
| `scale_down_duration_s` | Czas scale-downu Helm/StatefulSet dla fazy `down` | `reshard_summary.csv` |
| `wait_check_duration_s` | Pozostaly czas operacji, glownie oczekiwanie i sprawdzanie zdrowia klastra miedzy krokami | `reshard_summary.csv` |
| `auto_rebalance_status` | Status obserwacji auto-rebalance: `complete`, `already_complete`, `timeout` albo `partial` | `reshard_summary.csv` |
| `slots_on_new_after` | Liczba slotow na nowym masterze po obserwacji auto-rebalance | `reshard_summary.csv` |

### Wynik do analizy

```bash
python cli.py reshard --input ./results/valkey_reshard --output-dir ./plots/valkey_reshard
```

**Schemat CSV**: `file, run, phase, operation_duration_s, scale_duration_s, rebalance_duration_s, reshard_down_duration_s, del_node_duration_s, scale_down_duration_s, wait_check_duration_s, auto_rebalance_status, slots_on_new_after, expected_slots_on_new, ...`

**Analiza statystyczna**:
- `operation_duration_s` per run i faza (`up`, `down`)
- Komentarz jakosciowy: liczba krokow manualnych vs managed service (H4)

Wykresy czasowe oznaczaja liniami poczatek i koniec operacji reshardingu oraz podfazy scale/auto-rebalance.

**Wykresy**:
- `reshard_run_N.png`: ops/sec i p99/p50 z zaznaczonym oknem rebalancingu
- `reshard_down_run_N.png`: ops/sec i p99/p50 z zaznaczonym oknem reshard-down
- `reshard_comparison.png`: stacked operation duration, panel `up` i panel `down` obok siebie
- `reshard_auto_rebalance_N.csv`: trace obserwacji auto-rebalance (`second`, sloty na nowym masterze, stan klastra)

---

## 7. Backup & Restore (H4)

### Cel

Zmierzyc czas potrzebny na backup (BGSAVE) i pelne odtworzenie danych (restore) po ubiciiu wszystkich podow Valkey, przy zachowaniu PVC. Sluzy do porownania z Memorystore Point-in-Time Recovery.

### Procedura

```bash
N=5 SIZE_MB=100 bash k8s/scripts/backup_restore_benchmark.sh 100 ./results/valkey_backup
```

Zalecane rozmiary datasetow do testow: 100 MB, 1024 MB (1 GB), 10240 MB (10 GB).

> **Wazna uwaga dotyczaca SIZE_MB**: parametr `--target-mb` w `backup_restore_seed.py` oznacza calkowity rozmiar danych na caly klaster (nie per shard). Dane sa rozkladane rownomiernie po wszystkich shardach, wiec kazdy shard bedzie przetwarzal ~`SIZE_MB/3` danych. Dokumentuj to wyraznie w opisie eksperymentu.

**Sekwencja (per run)**:
1. **Seed**: zapisz `SIZE_MB * 1024 * 1024 / 1024 = SIZE_MB * 1024` kluczy (po 1 KB kazdy) do klastra Valkey.
2. **BGSAVE**: wyzwol BGSAVE na wszystkich masterach; czekaj na zakonczenie.
3. **Kill**: `kubectl delete pods -l app.kubernetes.io/component=valkey` (PVC sa zachowane).
4. **Restore**: czekaj az StatefulSet wstanie i klaster osiagnie `cluster_state:ok` z >= 3 masterami.
5. **Verify**: sprawdz losowa probe 10% kluczy (min 100); zapisz `integrity_ok`.
6. **Cleanup**: usun klucze testowe.

### Metryki zbierane

| Metryka | Definicja | Zrodlo |
|---|---|---|
| `save_duration_s` | Czas trwania BGSAVE (od wywolania do zakonczenia) | `backup_timing_{size}_{N}.json` |
| `restore_duration_s` | Czas od `kubectl delete pods` do `cluster_state:ok` z >= 3 masterami | `backup_timing_{size}_{N}.json` |
| `integrity_ok` | `True` jezeli 10% sample weryfikuje sie bez bledow | `backup_timing_{size}_{N}.json` |
| `seed_keys` | Liczba zapisanych kluczy | `backup_timing_{size}_{N}.json` |

### Wynik do analizy

```bash
python cli.py backup --input ./results/valkey_backup --output-dir ./plots/valkey_backup
```

**Schemat CSV**: `file, run, size_mb, seed_keys, save_duration_s, restore_duration_s, integrity_ok`

**Analiza statystyczna**:
- `save_duration_s` i `restore_duration_s` mean ± std per rozmiar datasetu
- Porownanie restore_duration_s (Valkey self-hosted) vs Memorystore PITR (jezeli dostepne)

**Wykresy**:
- `backup_restore_bars.png`: BGSAVE vs restore duration (slupki per rozmiar datasetu)
- `restore_by_size.png`: czas restore vs rozmiar datasetu (scatter + linia mean)

---

## 8. Analiza kosztow operacyjnych (H2, H4)

### H2 -- Koszty finansowe

Porownanie miesiecznego kosztu:

| System | Sposob obliczenia |
|---|---|
| Valkey self-hosted na GKE | Koszt wezlow K8s * udzial zasobow (CPU, RAM, dysk PVC) |
| Memorystore (Redis) | Cena z cennika GCP per GB pamiec per godzina * 730h/miesiac |

Zrodla danych: GCP Pricing Calculator, eksport billing z Cloud Console.

### H4 -- Nakład pracy operacyjnej

Miernik ilosci krokow dla 3 scenariuszy:

| Scenariusz | Valkey self-hosted | Memorystore |
|---|---|---|
| Upgrade | Kroki recznie (zmiana image.tag + helm upgrade + monitor) | Managed (1 klik lub API call) |
| Resharding | Kroki recznie (helm upgrade + obserwacja auto-rebalance + monitor) | Managed (skalowanie) |
| Backup/Restore | Kroki recznie (BGSAVE + kill + czekaj + verify) | PITR z konsoli/API |

Wynik: tabela porownawcza liczby krokow i czasu potrzebnego operatorowi.

---

## 9. Analiza statystyczna -- wskazowki ogolne

### Liczba powtorzen

Kazdy test liczbowy powtarzany **N=5** razy (minimum). Dla testow z wysoka wariancja (failover, consistency) zalecane N>=10.

### Raportowanie wynikow

Dla kazdej metryki podac:
- Srednia arytmetyczna (mean)
- Odchylenie standardowe (std)
- Przedzial ufnosci 95% (CI): `mean ± 1.96 * std / sqrt(N)`

### Testy statystyczne dla H1 i H3

Przy porownaniu Valkey vs Redis (lub Memorystore) uzyc:
- **Testu Shapiro-Wilka** do weryfikacji normalnosci rozkladu
- Jesli normalny: **t-test dla dwoch niezaleznych prob** (welch t-test bez zalozenia rownych wariancji)
- Jesli nienormalny: **test Manna-Whitneya U**
- Poziom istotnosci: alpha = 0.05
- Raportowac wartosc p i wielkosc efektu (Cohen's d lub rank-biserial correlation r)

### Wizualizacja

- Slupki z error bars (mean ± std)
- Szeregi czasowe z pionowymi annotacjami (chaos injected, chaos ended)
- Heatmapy dla bench wydajnosci

---

## 10. Znane problemy i ograniczenia (audyt kodu)

| Problem | Plik | Status |
|---|---|---|
| Domyslny `CHAOS_YAML` wskazuje na niepoprawny plik (`network-partition.yaml` zamiast `client-network-partition.yaml`) -- skrypt nigdy nie znajdzie zasobu `valkey-network-partition` | `k8s/scripts/consistency_benchmark.sh` linia 10 | **NAPRAWIONY** |
| Literowka `newtork-partition` w nazwie zasobu NetworkChaos | `k8s/chaos/network-partition.yaml` | **NAPRAWIONY** |
| `N=1` domyslnie w `failover_benchmark.sh` -- niewystarczajace dla analizy statystycznej | `k8s/scripts/failover_benchmark.sh` linia 4 | Uruchamiac z `N=5` explicite |
| Weryfikacja danych w backup to tylko 10% sample -- mozna przeoczyc utrate danych | `k8s/images/backup/backup_restore_seed.py` linia 101 | Ograniczenie metodologiczne; dokumentowac w pracy |
| `target_mb` w seed jest calkowity, nie per shard -- `help text` mowi "per shard" (mylace) | `k8s/images/backup/backup_restore_seed.py` linia 43 | Dokumentowac dokladna definicje w pracy |
| Hardcoded stress window w analizie (`STRESS_START_SECOND=30`, durations 30s/60s) musi byc zsynchronizowany z YAML | `src/resilience.py` linia 18-21 | Nie modyfikowac czasow bez aktualizacji obu miejsc |
| PromQL regex `valkey-[0-5]` nie obejmuje podow po scale-up do 4 shardow | `src/models.py` | Uruchamiac reshard i benchmark w oddzielnych sesjach |
| Brak warm-up przed benchmarkiem wydajnosci -- pierwszy run moze byc wyzszy/nizszy przez cold cache | `k8s/images/memtier/entrypoint.sh` | Dodac warm-up run recznie przed uruchomieniem `run_benchmark.sh` |
| Split-brain test wymaga Chaos Mesh i etykietowania podow | `k8s/scripts/split_brain_benchmark.sh` | Skrypt automatyzuje labelowanie; wymaga Chaos Mesh |
