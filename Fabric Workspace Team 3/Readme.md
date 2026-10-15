This is an auto-created file for Fabric Workspace Team 3

## Items

| Item | Purpose |
|---|---|
| `Taxi.Lakehouse` | Lakehouse for the NYC TLC taxi data (bronze files under `Files/bronze/nyctaxi`) |
| `PL_Bronze_NYCTaxi.DataPipeline` | Copies the TLC trip parquet files (yellow + green) and zone reference files into bronze, as-is |

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

Files are overwritten, so a re-run is safe. For a scheduled refresh, set `startMonth`/`monthCount`
to the recent months only (TLC publishes with a ~2 month delay).

## IDs referenced by the pipeline (not managed by git)

Connections are not stored in git; the pipeline JSON refers to them (and to the lakehouse) by GUID.

| What | Name | ID |
|---|---|---|
| Workspace | Alistar BU Data Platform Team 3 | `1b47e79e-9cb3-420c-8316-492b02b75565` |
| Lakehouse | Taxi | `b58a5437-69fc-4ce4-939e-bb95fc82cd34` |
| HTTP connection (anonymous) | TLC_Trip_Record_Data — `https://d37ci6vzurychx.cloudfront.net/trip-data/` | `f7d3f10f-f31d-4fdc-8eae-c6d7c27ceeaf` |
| HTTP connection (anonymous) | TLC_Miscellaneous_Data — `https://d37ci6vzurychx.cloudfront.net/misc/` | `8c74ff85-2c23-4dfe-9dd3-8c78c48a0c89` |

The connections' base URLs already include `trip-data/` / `misc/`, so the copy activities use only
the file name as relative URL. If the lakehouse is ever recreated, its ID changes and the pipeline
must be updated.
