## Weather API Pipeline

### Overview
- Fetches daily weather forecasts from OpenWeather for a curated list of locations.
- Normalizes timestamps to UTC and returns a tabular DataFrame.
- Stores raw API JSON to GCS for lineage and auditability.
- Stores transformed data to GCS (JSON.gz) and loads it into BigQuery.
- Emits completeness and data‑quality metrics and a deterministic logical key for idempotency.

### Architecture
```
OpenWeather API
      │
      ▼
WeatherFetcher ──▶ normalize (in pipeline) ──▶ GCSStorage (JSON.gz) ──▶ BigQueryLoader (append)
      │                │                          ▲                         ▲
      └── raw JSON ────┴──────────────────────────┘                         │
                    metrics & logs                                       table
```

### Components
- Fetcher (`fetcher.py`)
  - Calls OpenWeather One Call endpoint per location.
  - Shapes daily forecasts into rows; adds UTC `forecast_date` and `logical_key`.
  - Logs per‑location completeness (expected vs available vs fetched days) and defaulted field counts.

- Pipeline (`pipeline.py`)
  - Orchestrates: fetch → normalize timestamps to UTC → store.
  - Structured logging, run lifecycle messages, error handling.

- Storage (`gcs_storage.py`)
  - Raw: stores full API responses per run as gzipped JSON.
  - Processed: serializes DataFrame to JSON (records, ISO timestamps), compresses with gzip.
  - Uploads with `content_encoding=gzip`.

- BigQuery loader (`bigquery_loader.py`)
  - Appends transformed rows to `project.dataset.table` using `load_table_from_dataframe`.
  - Autodetects schema; logs destination and row counts.

- Configuration (`config.py`)
  - Pydantic models (`Location`, `WeatherConfig`) with strict validation (ranges for lat/lon, URL validation, immutability).
  - Required runtime fields: `api_key`, `api_base_url`, `gcs_bucket`; knobs: `forecast_days`, `locations`.

### Data flow
1. Iterate configured `locations` and call OpenWeather.
2. Build one row per location×day up to `forecast_days`.
3. Normalize timestamps (UTC), compute `forecast_date` from API epoch, and add `logical_key`.
4. Emit completeness metrics (per location) and a final summary.
5. Write raw API JSON to GCS (timestamped object in `weather_raw/`).
6. Write transformed JSON.gz to GCS (timestamped object in `weather_data/`).
7. Load transformed DataFrame into BigQuery (append).

### Data model (key columns)
- Identity: `logical_key` ("city|country|YYYY-MM-DD"), `city`, `country`, `latitude`, `longitude`.
- Dates/times: `forecast_date` (UTC date), `forecast_timestamp` (epoch→datetime), `sunrise`, `sunset`, `ingestion_timestamp`.
- Weather: `max_temp`, `min_temp`, `day_temp`, `night_temp`, `feels_like_day`, `feels_like_night`, `humidity`, `wind_speed`.
- Conditions: `weather_main`, `weather_description`, `weather_icon`, `precipitation_probability`, `rain`, `uvi`, `clouds`.

### Design patterns and practices
- Separation of concerns: fetch, orchestrate, store in distinct classes.
- Configuration via Pydantic with strict schemas (immutable models, extra=forbid).
- Observability: structured logs, per‑location completeness, final summary (coverage %, defaults filled, missing % per field).
- Idempotency: deterministic `logical_key` for downstream dedup/merge.
- Reliability: request timeouts, narrow exception handling, graceful degradation per location.
- Data hygiene: UTC‑normalized timestamps; explicit defaults for optional fields.
- Error handling: try/except blocks & partial failures don't stop the entire pipeline.

### Running
Prerequisites
- Python 3.12+, `requests`, `pandas`, `google-cloud-storage`.
- GCP auth (ADC) and a GCS bucket.
- OpenWeather API key.

Environment (examples)
```
export WEATHER_API_KEY=...
export GCS_BUCKET_NAME=your-bucket
export BQ_PROJECT_ID=your-project
export BQ_STAGING_DATASET_ID=your_dataset
export BQ_WEATHER_TABLE_ID=weather_forecast
```

Programmatic usage
```python
from pipelines.weather_api_pipeline import WeatherPipeline
from pipelines.weather_api_pipeline.config import WeatherConfig, Location

config = WeatherConfig(
    api_key="${WEATHER_API_KEY}",
    api_base_url="https://api.openweathermap.org",
    gcs_bucket="your-bucket",
    forecast_days=2,
    locations=[
        Location(city="London", country="UK", latitude=51.5074, longitude=-0.1278),
    ],
)
WeatherPipeline(config).run()
```

Object naming
- Raw: `weather_raw/{YYYYMMDD_HHMMSS}_raw_responses.json.gz`
- Processed: `weather_data/{YYYYMMDD_HHMMSS}_weather_forecast.json.gz`

```python
from pipelines.weather_api_pipeline import run_pipeline
run_pipeline()
```


### Monitoring Considerations

1. **Pipeline Success/Failure**:
   - The pipeline logs completion status

2. **Data Quality**:
   - Row counts are logged
   - API response validation is performed

### Maintenance Tasks

1. **API Updates**:
   - If the OpenWeather API changes, update the fetcher module

2. **Destination List Updates**:
   - Edit the TRAVEL_DESTINATIONS list in pipeline.py to add/remove locations

3. **Schema Evolution**:
   - If new fields are needed, update the BigQuery schema in pipeline.py

## Future Enhancements

Potential improvements for future versions:

1. **Dynamic Destination List**:
   - Load travel destinations from a configuration file or database

2. **Advanced Error Recovery**:
   - Implement retries for failed API requests
   - Add dead-letter queue for failed records

3. **Performance Optimization**:
   - Parallel API requests for faster data collection
   - Batched BigQuery loading

4. **Monitoring Integrations**:
   - Add integration with monitoring systems
   - Implement alerting for pipeline failures

5. **Data Quality Checks**:
   - Add validation rules for incoming data
   - Implement anomaly detection

## Conclusion

The Weather API Pipeline provides a robust solution for collecting, processing, and storing weather forecast data for travel planning. Its modular design, error handling, and adherence to best practices make it maintainable and extensible for future needs. 