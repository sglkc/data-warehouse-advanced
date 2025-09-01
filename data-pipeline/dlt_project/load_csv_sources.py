import os

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import filesystem, read_csv

pg_connection = os.environ.get("POSTGRES_CONNECTION_URL")

def load_csv() -> None:
    if not pg_connection:
        print("POSTGRES_CONNECTION_URL not set!")
        return

    source = filesystem(
        bucket_url="./sources",
        file_glob="*.csv"
    ) | read_csv()

    pipeline = dlt.pipeline(
        pipeline_name="load_csv_sources",
        destination=postgres(credentials=pg_connection)
    )

    info = pipeline.run(source)

    print(info)


if __name__ == "__main__":
    load_csv()
