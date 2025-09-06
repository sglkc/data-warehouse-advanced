import os

import dlt
from dlt.destinations import postgres
from dlt.sources.filesystem import filesystem, read_csv

pg_connection = os.environ.get("POSTGRES_CONNECTION_URL", "")

sources = [
    { "file": "Customer", "table": "customers", "merge_key": "CustomerID" },
    { "file": "Employee", "table": "employees", "merge_key": "EmployeeID" },
    { "file": "OrderDetails", "table": "order_details", "merge_key": "OrderDetailsID" },
    { "file": "Orders", "table": "orders", "merge_key": "OrderID" },
    { "file": "Product", "table": "products", "merge_key": "ProductID" },
    { "file": "Region", "table": "regions", "merge_key": "RegionID" },
    { "file": "Warehouse", "table": "warehouses", "merge_key": "WarehouseID" },
]

def load_csv(
    file: str,
    table: str,
    merge_key: str,
) -> None:
    source = filesystem(
        bucket_url="./sources",
        file_glob=f"{file}.csv",
    ) | read_csv()

    _ = source.apply_hints(
            # https://dlthub.com/docs/general-usage/incremental-loading
            # use "merge" to update existing data and add new data without truncating
            write_disposition="merge",
            # merge_key isn't always primary_key, but the data is simple
            primary_key=merge_key,
            merge_key=merge_key,
        )

    pipeline = dlt.pipeline(
        destination=postgres(credentials=pg_connection),
        # schema name in pgsql to insert into
        dataset_name="staging",
        pipeline_name=f"load_{table}_staging",
    )

    info = pipeline.run(source.with_name(table))

    print(f"{file} load info:", info)
    print()

if __name__ == "__main__":
    for source in sources:
        load_csv(source["file"], source["table"], source["merge_key"])
