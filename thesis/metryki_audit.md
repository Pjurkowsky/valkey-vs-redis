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
