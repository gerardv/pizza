This is an auto-created file for Fabric Workspace Team 3

## Items

| Item | Purpose |
|---|---|
| `Taxi.Lakehouse` | Lakehouse for the NYC TLC taxi data (bronze files under `Files/bronze/nyctaxi`) |
| `PL_Orchestrate_NYCTaxi.DataPipeline` | Entry point: bronze ingestion, then silver staging, for one month range. Schedule this one |
| `PL_Bronze_NYCTaxi.DataPipeline` | Copies the TLC trip parquet files (yellow + green) and zone reference files into bronze, as-is |
| `NB_Silver_NYCTaxi.Notebook` | Bronze files to Delta tables `silver_trips` and `silver_taxi_zone` |

## Orchestrator

```
Set start month ─▶ Bronze ingestion (PL_Bronze_NYCTaxi) ─▶ Silver staging (NB_Silver_NYCTaxi)
                                          (on success; room for checks in between)
```

| Parameter | Default | Meaning |
|---|---|---|
| `startMonth` | *(empty)* | Empty = rolling: the `monthCount` full months before the current month |
| `monthCount` | `3` | Number of months |

Full (re)load: `startMonth = 2022-01`, `monthCount = 48`. Months TLC has not published yet (HTTP 404)
are skipped by bronze and silver, and picked up by a later run.

## Bronze layout

```
Files/bronze/nyctaxi/
  yellow/<yyyy>/yellow_tripdata_<yyyy-MM>.parquet
  green/<yyyy>/green_tripdata_<yyyy-MM>.parquet
  reference/taxi_zone_lookup.csv, taxi_zones.zip
```

## Pipeline parameters

| Parameter | Default | Meaning |
|---|---|---|
| `startMonth` | `2022-01` | First month to fetch (`yyyy-MM`) |
| `monthCount` | `48` | Number of months from `startMonth` (48 = 2022 through 2025) |
| `referenceFiles` | `["taxi_zone_lookup.csv","taxi_zones.zip"]` | Files under `/misc/` on the TLC CDN |

Files are overwritten, so a re-run is safe. Normally started by the orchestrator, which passes
`startMonth`/`monthCount`.

## IDs referenced by the pipeline (not managed by git)

Connections are not stored in git; the pipeline JSON refers to them (and to the lakehouse) by GUID.

| What | Name | ID |
|---|---|---|
| Workspace | Alistar BU Data Platform Team 3 | `1b47e79e-9cb3-420c-8316-492b02b75565` |
| Lakehouse | Taxi | `b58a5437-69fc-4ce4-939e-bb95fc82cd34` |
| Pipeline | PL_Bronze_NYCTaxi (invoked by the orchestrator) | `8950d7c2-061c-4d00-aaa4-b505beda6b5e` |
| Notebook | NB_Silver_NYCTaxi (invoked by the orchestrator) | `c5d7c039-6c06-452c-a713-88a59b1eb9bc` |
| HTTP connection (anonymous) | TLC_Trip_Record_Data — `https://d37ci6vzurychx.cloudfront.net/trip-data/` | `f7d3f10f-f31d-4fdc-8eae-c6d7c27ceeaf` |
| HTTP connection (anonymous) | TLC_Miscellaneous_Data — `https://d37ci6vzurychx.cloudfront.net/misc/` | `8c74ff85-2c23-4dfe-9dd3-8c78c48a0c89` |

The connections' base URLs already include `trip-data/` / `misc/`, so the copy activities use only
the file name as relative URL. If the lakehouse is ever recreated, its ID changes and the pipeline
must be updated.
