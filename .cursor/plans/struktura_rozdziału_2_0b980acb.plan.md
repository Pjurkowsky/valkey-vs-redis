---
name: Struktura rozdziału 2
overview: Propozycja struktury rozdziału 2 "Teoretyczne podstawy problematyki badań", opartej na tematyce poruszanej w pozostałych rozdziałach pracy (metodologia, wyniki, hipotezy).
todos:
  - id: write-ch2-cache
    content: Napisać sekcję 2.1 o systemach cache
    status: completed
  - id: write-ch2-redis
    content: Napisać sekcję 2.2 o architekturze Redis
    status: completed
  - id: write-ch2-valkey
    content: Napisać sekcję 2.3 o Valkey
    status: completed
  - id: write-ch2-license
    content: Napisać sekcję 2.4 o zmianie licencji
    status: completed
  - id: write-ch2-k8s
    content: Napisać sekcję 2.5 o Kubernetes
    status: completed
  - id: write-ch2-managed
    content: Napisać sekcję 2.6 o usługach zarządzanych vs self-hosted
    status: completed
  - id: write-ch2-related
    content: Napisać sekcję 2.7 przegląd prac pokrewnych
    status: completed
  - id: update-bib
    content: Uzupełnić bibliography.bib o nowe źródła
    status: completed
isProject: false
---

# Struktura rozdziału 2: Teoretyczne podstawy problematyki badań

## Kontekst

Rozdział 1 (Wstęp) w sekcji "Struktura pracy" deklaruje, że rozdział 2 powinien zawierać:

> "omówienie systemów cache'owania, architektury Redis i Valkey, modeli klastrowych oraz znaczenia zmian licencyjnych dla ekosystemu open-source"

Rozdział 3 (Metodologia) zakłada znajomość pojęć: klaster Valkey, shardy, sloty, BGSAVE, pliki RDB, StatefulSet, PVC, Helm chart, Memorystore, tryb atomowej migracji slotów. Rozdział 4 (Wyniki) operuje pojęciami resharding, rolling upgrade, failover, cluster_state:ok, percentyle opóźnień.

## Proponowana struktura sekcji

### 2.1 Systemy cache'owania w pamięci operacyjnej

- Rola cache w architekturze aplikacji (zmniejszenie latencji, odciążenie bazy)
- Strategie cache: cache-aside, read-through, write-through, write-behind
- Wymagania wobec systemów cache: niska latencja, wysoka przepustowość, trwałość opcjonalna, skalowalność horyzontalna
- Metryki oceny: ops/sec, latency percentiles (p50, p95, p99, p99.9)

### 2.2 Architektura Redis

- Model danych (klucz-wartość, struktury: string, hash, list, set, sorted set)
- Jednowątkowość i model obsługi żądań (event loop)
- Mechanizmy trwałości: RDB (point-in-time snapshot, BGSAVE) i AOF (append-only file)
- Replikacja master-replica, Redis Sentinel jako mechanizm HA
- Redis Cluster: sharding oparty o hash sloty (16384 slotów), przekierowania MOVED/ASK, resharding

### 2.3 Valkey -- geneza i różnice względem Redis

- Okoliczności powstania forka (zmiana licencji Redis 7.2 -> SSPL/RSALv2, marzec 2024)
- Linux Foundation jako steward projektu
- Zgodność protokołu i poleceń z Redis
- Nowe mechanizmy: atomowa migracja slotów (od wersji 8.0/9.0), io-threads improvements
- Ekosystem narzędzi: valkey-cli, valkey-benchmark, kompatybilność z bibliotekami klienckimi Redis

### 2.4 Zmiana licencji Redis i jej wpływ na ekosystem

- Chronologia: BSD -> SSPL+RSALv2 (Redis 7.4+)
- Motywacja Redis Ltd (ochrona przed cloud providers)
- Reakcja społeczności i dostawców chmurowych (AWS, GCP, Azure)
- Konsekwencje dla użytkowników: ograniczenia redystrybucji, hosting, usługi zarządzane
- Odniesienie do pracy Han i współautorów [han2026]

### 2.5 Kubernetes jako środowisko uruchomieniowe baz danych

- Koncepcja StatefulSet: stabilna tożsamość podów, porządek uruchamiania/zamykania
- Persistent Volume Claims (PVC) i klasy storage
- Helm jako narzędzie zarządzania konfiguracją (chart, values, upgrade, rollback)
- Wyzwania: rolling update stanowego workloadu, odkrywanie klastra, sieć wewnętrzna (headless service)
- Mechanizmy odtwarzania: liveness/readiness probes, restart policies

### 2.6 Usługi zarządzane a rozwiązania self-hosted

- Model odpowiedzialności: shared responsibility w usługach zarządzanych vs pełna kontrola self-hosted
- Google Cloud Memorystore for Redis: architektura, ograniczenia, dostępne operacje (backup, scaling, upgrade)
- Trade-off: koszt operacyjny vs kontrola vs elastyczność konfiguracji
- Odniesienie do pracy Awaysheha i Awada [awaysheh2025]

### 2.7 Przegląd prac pokrewnych

- Istniejące benchmarki Redis/Valkey (jeśli dostępne w literaturze)
- Porównania systemów in-memory w kontekście chmurowym
- Luka badawcza: brak porównań uwzględniających jednocześnie wydajność, operacyjność i koszty Valkey vs Redis w Kubernetes

## Uwagi dotyczące stylu

- Każda sekcja powinna kończyć się podsumowaniem w 1-2 zdaniach wskazującym, dlaczego dany temat jest istotny dla dalszych rozdziałów
- Cytowania z bibliografii: han2026 (licencja), awaysheh2025 (Valkey jako chmurowo-natywny store), kaptosv2025 (cache w web apps)
- Należy uzupełnić bibliografię o źródła dotyczące Redis Cluster (dokumentacja), Kubernetes StatefulSets, Valkey release notes
- Zakres: ~15-20 stron (proporcjonalnie do rozdziału 3 który ma ~10 stron gęstego tekstu z listingami)

