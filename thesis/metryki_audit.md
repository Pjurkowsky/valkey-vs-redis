# Audyt tabeli 3.3 (Metryki badawcze) vs. rozdział 4 (Wyniki)

## 1. Metryki z tabeli 3.3 faktycznie użyte w wynikach

| Metryka z tabeli | Gdzie w rozdziale 4 |
|---|---|
| Ops/sec, średnia, odchylenie std. | Wszędzie -- perf, reshard, failover, resilience, upgrade |
| Różnica procentowa | Stosunek VK/RD (tab. 4.1), przyspieszenie (tab. 4.2) |
| p50, p95, p99, p99.9 | Reshard (tab. 4.4) |
| p99 | Perf, failover, resilience CPU, resilience pamięć, upgrade |
| Czas degradacji | Failover (tab. 4.3) |
| Liczba błędów klienta | Failover ("Nieudane żąd."), upgrade ("Błędy", "Timeouty") |
| Utracone potwierdzone zapisy, loss rate | Split-brain (tab. 4.5) |
| Liczba błędów podczas partycji | Split-brain ("Odrzucone") |
| Czas procedury | Upgrade (czas [s]), reshard (czas operacji), backup/restore (fazy) |

## 2. Metryki z tabeli 3.3 NIEUŻYTE w wynikach

| Metryka | Status |
|---|---|
| Średnie zużycie CPU | Brak w rozdziale 4 |
| Zużycie CPU per pod | Brak w rozdziale 4 |
| Zużycie pamięci | Tylko w resilience (wypełnienie maxmemory), brak ogólnego monitoringu |
| Restarty podów | Wspomniane jedynie w resilience CPU jako "nie wykryto restartu poda", brak systematycznego raportowania |
| Czas odtworzenia mastera | Brak jawnie -- jest "czas degradacji", ale nie czas promocji repliki |
| Czas powrotu do stabilnego throughputu | Brak jako osobna metryka -- jest tylko "Powrót 5/5" (bool) |
| Liczba kroków | Brak w rozdziale 4 |
| Liczba komend | Brak w rozdziale 4 |
| Liczba decyzji operatora | Brak w rozdziale 4 |
| Możliwość rollbacku | Brak w rozdziale 4 |
| Koszt miesięczny | Brak w rozdziale 4 |
| Koszt na 100 tys. ops/sec | Brak w rozdziale 4 |
| Koszt pracy operacyjnej | Brak w rozdziale 4 |
| Koszt zasobów infrastruktury | Brak w rozdziale 4 |

## 3. Metryki użyte w wynikach, ale NIEOBECNE w tabeli 3.3

| Metryka | Gdzie w rozdziale 4 |
|---|---|
| Stosunek/przyspieszenie (ratio) | Perf, reshard |
| Wykrycia degradacji (np. 4/5) | Failover, resilience CPU |
| Powrót do baseline (np. 5/5) | Resilience CPU |
| Eksmisje (evictions) | Resilience pamięć |
| Błędy OOM | Resilience pamięć |
| CLUSTERDOWN (count) | Failover, split-brain |
| Error rate [%] | Upgrade |
| Minority/majority loss rate | Split-brain |
| Fazy czasowe (BGSAVE, snapshot PVC, kopiowanie RDB...) | Backup/restore |
| Zgodność liczby kluczy po restore | Backup/restore |

## 4. Realizacja po audycie

Metryki dopisane do analizy benchmarku wydajnosciowego:

| Metryka | Status | Artefakt |
|---|---|---|
| Srednie zuzycie CPU | Dodane z Prometheusa dla benchmarkow memtier | `plots/benchmark/*/summary.csv`, kolumny `cpu_util_mean`, `cpu_util_std` |
| Zuzycie CPU per pod | Dodane z Prometheusa | `plots/benchmark/*/cpu_util_per_pod.csv` |
| Zuzycie pamieci | Dodane z Prometheusa | `plots/benchmark/*/summary.csv`, kolumny `memory_usage_mean`, `memory_usage_std` |
| Zuzycie pamieci per pod | Dodane z Prometheusa | `plots/benchmark/*/memory_usage_per_pod.csv` |
| Restarty podow | Dodane z Prometheusa, jezeli dostepna jest metryka `kube_pod_container_status_restarts_total` | `plots/benchmark/*/summary.csv`, kolumny `pod_restarts_mean`, `pod_restarts_std` |

Metryki, ktorych nie da sie wiarygodnie odzyskac z samych istniejacych wynikow memtier:

| Metryka | Dlaczego wymaga ponownego testu albo dodatkowej instrumentacji |
|---|---|
| Czas odtworzenia mastera | Wymaga timestampu zdarzenia awarii/promocji repliki z Kubernetes/Redis/Valkey albo monitoringu cluster topology w trakcie testu. Sam wynik memtier ma tylko okno workloadu. |
| Czas powrotu do stabilnego throughputu | Wymaga jawnej definicji stabilnosci i per-second throughput/baseline w scenariuszu awarii; w benchmarku wydajnosciowym nie ma zdarzenia awarii. |
| Liczba krokow | Metryka proceduralna; trzeba policzyc z opisanej procedury/operator runbooka, a nie z JSON memtier. |
| Liczba komend | Metryka proceduralna; wymaga instrumentacji skryptu lub audytu komend w scenariuszu operacyjnym. |
| Liczba decyzji operatora | Metryka proceduralna/subiektywna; wymaga zdefiniowania modelu decyzyjnego. |
| Mozliwosc rollbacku | Wymaga oceny procedury i narzedzi, nie wynika automatycznie z wynikow wydajnosci. |
| Koszt miesieczny | Wymaga aktualnych cennikow i zalozen infrastrukturalnych. |
| Koszt na 100 tys. ops/sec | Wymaga kosztu miesiecznego oraz wybranego throughputu referencyjnego. |
| Koszt pracy operacyjnej | Wymaga zalozenia stawki godzinowej i czasu pracy operatora. |
| Koszt zasobow infrastruktury | Wymaga aktualnych cen VM/dyskow/sieci/managed service oraz regionu. |

## 5. Przygotowane metryki proceduralne i kosztowe

Uzupelniajace artefakty zostaly przygotowane jako osobne pliki:

| Metryka | Artefakt | Uwagi |
|---|---|---|
| Liczba krokow | `thesis/src/images/procedural_metrics.csv` | Policzona z procedur opisanych w rozdziale 3. |
| Liczba komend | `thesis/src/images/procedural_metrics.csv` | Liczone sa operator-visible command sites, bez iteracji petli, retry i powtorzen `N`. |
| Koszt miesieczny | `thesis/src/images/cost_metrics.csv` | Dla self-hosted jako formula z parametrem ceny `n2-standard-4`; dla Memorystore policzony z cennika. |
| Koszt na 100 tys. ops/sec | `thesis/src/images/cost_metrics.csv` | Osobno dla bazowego testu Valkey/Redis i dla scenariusza reshardingu obejmujacego Memorystore. |
| Koszt zasobow infrastruktury | `thesis/src/images/cost_assumptions.csv`, `thesis/src/images/cost_metrics.csv` | Obejmuje GKE fee, VM, boot disks, PVC albo wezly Memorystore; nie obejmuje rabatow, sieci, backup storage i AOF. |

Opis metodologii znajduje sie w `thesis/metryki_operacyjne_koszty.md`.
