# Set up data pipeline

Source data: `./data/HomeAssignmentEvents.jsonl`.
Architecture:

- data/output.db
  - DuckDB database to store the ingested data.
- futurae-assignment/
  - models.py
    - Pydantic models to represent the data structure of the events. Include an invalid model to capture parsing errors.
  - exceptions.py
    - Centralized place to define custom exceptions for the project.
  - db.py
    - Implement a duckdb client to query and ingest data.
  - streamer.py
    - Reads the raw JSONL data from the file and yields one event dictionary at a time for processing.
  - parser.py
    - Parses the source dict from the streamer and validates it against the Pydantic models defined in `models.py`.
    - Yields both valid and invalid events, where invalid events are captured using the invalid model.
  - loader_raw.py
    - Ingests valid events into the events table in the DuckDB database.
    - Ingests invalid events into events_invalid table in the DuckDB database.
