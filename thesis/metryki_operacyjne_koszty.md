# Metryki proceduralne i kosztowe

Ten plik przygotowuje brakujące metryki z audytu tabeli 3.3. Nie są one wyciągane z JSON-ów `memtier`, bo opisują procedurę operatora i koszt środowiska, a nie pojedynczy przebieg workloadu.

## Zakres przygotowania

Artefakty:

- `thesis/src/images/procedural_metrics.csv` -- liczba kroków i liczba komend dla scenariuszy operacyjnych.
- `thesis/src/images/cost_assumptions.csv` -- jawne założenia kosztowe i źródła cen.
- `thesis/src/images/cost_metrics.csv` -- koszt miesięczny, koszt zasobów infrastruktury oraz formuły kosztu na 100 tys. ops/sec.

## Definicje

**Liczba kroków** oznacza liczbę logicznych faz procedury opisanej w rozdziale 3, np. przygotowanie danych, wykonanie operacji administracyjnej, oczekiwanie na gotowość i weryfikację.

**Liczba komend** oznacza liczbę operator-visible command sites w runbooku/procedurze. Nie liczę każdej iteracji pętli `wait`, każdego powtórzenia `N=5` ani wewnętrznych retry. Dzięki temu metryka opisuje złożoność procedury, a nie długość implementacji skryptu.

**Koszt miesięczny** jest kosztem steady-state dla aktywnego środowiska przez 730 h/miesiąc. Nie obejmuje rabatów, free tier, CUD/SUD, kosztów ruchu sieciowego, backup storage ani AOF persistence.

**Koszt zasobów infrastruktury** dla self-hosted obejmuje GKE management fee, węzły GKE, dyski boot oraz PVC. Dla Memorystore obejmuje węzły usługi zarządzanej.

**Koszt na 100 tys. ops/sec** jest liczony jako:

```text
monthly_cost_usd / (throughput_ops_sec / 100000)
```

## Koszty

Założenia stałe:

- region: `europe-central2` / Warsaw,
- GKE: 3 x `n2-standard-4`, boot disk `pd-balanced` 50 GiB na węzeł,
- self-hosted PVC: 6 x 8 GiB `pd-ssd`,
- Memorystore: 3 shardy, 1 replika na shard, łącznie 6 x `redis-standard-small`,
- miesiąc rozliczeniowy: 730 h.

Zweryfikowane z cenników Google:

- GKE cluster management fee: `0.10 USD / cluster-hour`,
- `pd-balanced`: `0.000136986 USD / GiB-hour`,
- `pd-ssd`: `0.000232877 USD / GiB-hour`,
- Memorystore `redis-standard-small` w `europe-central2`: `0.1425 USD / node-hour`.

Nie udało się automatycznie pobrać w tym środowisku aktualnej stawki `n2-standard-4` z Cloud SKU API, więc w CSV jest ona jawnie oznaczona jako `TO_VERIFY`. Formuły są już przygotowane:

```text
self_hosted_monthly_usd = 96.16 + 2190 * n2_standard_4_hourly_usd
memorystore_monthly_usd = 624.15
```

Próg opłacalności przy tych założeniach:

```text
n2_standard_4_hourly_usd = 0.2411
```

Jeżeli aktualna cena `n2-standard-4` w `europe-central2` jest niższa niż `0.2411 USD/h`, sam koszt infrastruktury self-hosted wychodzi niżej niż Memorystore. Jeżeli jest wyższa, Memorystore wychodzi taniej w steady-state, zanim doliczy się koszt pracy operatora.

## Uwaga metodologiczna

Dla Valkey i Redis 7.2 istnieje bazowy throughput z tabeli wydajnościowej: profil 1 KB, mixed 50/50, 2 vCPU. Te wartości pochodzą z lokalnego środowiska minikube, więc są porównywalne między Valkey i Redis 7.2, ale nie są uczciwą bazą do porównania z Memorystore.

Dla porównania obejmującego wszystkie trzy warianty użyłem dodatkowo throughputu z testu reshardingu 3 -> 4 w GCP. To nie jest throughput saturacyjny, tylko przepustowość w scenariuszu operacyjnym pod obciążeniem, dlatego w `cost_metrics.csv` jest oddzielny zakres `operational_reshard_cost_efficiency`.

## Co wymaga uzupełnienia

Do pełnej tabeli kosztowej przed finalnym oddaniem pracy trzeba tylko ręcznie potwierdzić aktualną cenę `n2-standard-4` dla `europe-central2` w Google Cloud SKUs i podstawić ją do formuł w `cost_metrics.csv`.

Jeżeli chcesz doliczyć koszt pracy operatora, potrzebne jest osobne założenie:

```text
operator_labor_monthly_usd = operator_hours_per_month * operator_hourly_rate_usd
```

Tego nie da się wiarygodnie odzyskać z wyników benchmarku bez przyjęcia stawki godzinowej i modelu dyżurów/utrzymania.
