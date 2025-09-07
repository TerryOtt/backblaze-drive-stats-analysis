import pyiceberg
import pyiceberg.catalog


def _load_catalog() -> pyiceberg.catalog.Catalog:
    warehouse_path = "/tmp/warehouse"
    catalog: pyiceberg.catalog.Catalog = pyiceberg.catalog.load_catalog(
        "default",
        **{
            'type'          : 'sql',
            "uri"           : f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse"     : f"file://{warehouse_path}",
        },
    )

    return catalog


def _main() -> None:
    print("Hello world")
    catalog = _load_catalog()

if __name__ == "__main__":
    _main()