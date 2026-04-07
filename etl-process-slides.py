import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full")


@app.cell
def _imports():
    from pathlib import Path

    import marimo as mo
    import pandas as pd
    import yaml

    return Path, mo, pd, yaml


@app.cell
def _load_config(Path, yaml):
    cfg_path = Path("config.yaml")
    cfg = {}
    if cfg_path.exists():
        with open(cfg_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    return (cfg,)


@app.cell
def _tables(cfg, pd):
    datasets = cfg.get("socrata", {}).get("datasets", {})
    data_files = cfg.get("data_input", {}).get("files", {})
    weather_url = cfg.get("open_meteo", {}).get(
        "archive_url", "https://archive-api.open-meteo.com/v1/archive"
    )

    sources_df = pd.DataFrame(
        [
            ["Socrata API", "Crashes", datasets.get("crashes", "h9gi-nx95"), "Unfallereignisse (Faktbasis)"],
            ["Socrata API", "Persons", datasets.get("persons", "f55k-p6yu"), "Beteiligte Personen"],
            ["Socrata API", "Vehicles", datasets.get("vehicles", "bm4k-52h4"), "Fahrzeugmerkmale und Faktoren"],
            ["Socrata API", "Precincts", datasets.get("precincts", "y76i-bdw7"), "Geometrie fuer raeumliche Zuordnung"],
            ["Lokale Datei", "crashes", data_files.get("crashes", "crashes.csv"), "Hauptinput ETL"],
            ["Lokale Datei", "persons", data_files.get("persons", "persons.csv"), "Hauptinput ETL"],
            ["Lokale Datei", "vehicles", data_files.get("vehicles", "vehicles.csv"), "Hauptinput ETL"],
            ["Wetter API", "Open-Meteo", weather_url, "Stundenwerte Wetterkontext"],
        ],
        columns=["Quelle", "Datensatz", "Identifier/Datei", "Verwendung"],
    )

    transform_examples_df = pd.DataFrame(
        [
            ["Spaltenvereinheitlichung", "CRASH DATE", "crash_date"],
            ["ID-Normalisierung", "1001.0", "1001 (Int64)"],
            ["Borough-Bereinigung", " queens ", "QUEENS"],
            ["Fahrzeugtyp-Bucketing", "Station Wagon/Sport Utility Vehicle", "SUV"],
            ["Faktor-Bereinigung", "Unspecified", "herausgefiltert"],
            ["Raeumliche Inferenz", "lat/lon + borough = NULL", "borough aus Precinct-Polygonen abgeleitet"],
        ],
        columns=["Transformation", "Vorher", "Nachher"],
    )

    entities_df = pd.DataFrame(
        [
            ["borough", "Lookup", "1 Zeile je Borough"],
            ["precinct", "Lookup", "1 Zeile je Precinct"],
            ["location", "Dimension", "1 Zeile je Unfallort"],
            ["crash", "Fakt", "1 Zeile je collision_id"],
            ["vehicle_type", "Lookup", "1 Zeile je Fahrzeugkategorie"],
            ["vehicle", "Dimension", "1 Zeile je vehicle_id"],
            ["person_type", "Lookup", "1 Zeile je Personentyp"],
            ["person", "Dimension", "1 Zeile je person_id"],
            ["contributing_factor", "Lookup", "1 Zeile je Faktor"],
            ["vehicle_factor", "Bridge", "vehicle_id x factor_id"],
            ["weather_observation", "Dimension", "borough x datum x stunde"],
        ],
        columns=["Entitaet", "Rolle", "Granularitaet"],
    )
    return entities_df, sources_df, transform_examples_df


@app.cell
def _slides(entities_df, mo, sources_df, transform_examples_df):
    process_md = mo.md(
        """
    ## Wie die Verarbeitung ablaeuft

    ### Extract
    - Lokale Dateien: `data/crashes.csv`, `data/persons.csv`, `data/vehicles.csv`
    - Socrata: `crashes`, `persons`, `vehicles`, `precincts`
    - Open-Meteo: stundenbasierte Wetterwerte nach Borough und Zeitraum

    ### Transform
    - Spaltennamen vereinheitlichen (snake_case)
    - IDs und Datentypen normalisieren
    - Ungueltige/nicht lokalisierbare Unfaelle filtern
    - Spatial Join: lat/lon -> precinct und borough
    - Entitaeten und Bridge-Tabellen aufbauen
    - Wetterbeobachtungen je borough/datum/stunde anreichern

    ### Load
    - `output/st_*.csv` und `output/*.parquet` schreiben
    - SQL-Modell: `sql/db_model_creation.sql`
    - Lade-Skript: `sql/load_staging_to_real.sql`
    - Analyse-Views: `sql/create_mdm_views.sql`
    """
    )

    sql_md = mo.md(
        """
    ## SQL-Ladebeispiel

    ```sql
    SET IDENTITY_INSERT [Crash] ON;
    INSERT INTO [Crash] ([collision_id], [crash_date], [crash_time], [location_id])
    SELECT
    TRY_CONVERT(INT, s.[collision_id]),
    TRY_CONVERT(DATE, s.[crash_date]),
    TRY_CONVERT(TIME, s.[crash_time]),
    TRY_CONVERT(INT, s.[location_id])
    FROM [st_crash] s;
    SET IDENTITY_INSERT [Crash] OFF;
    ```

    Zweck: IDs aus dem Staging bleiben erhalten, dadurch bleiben Fremdschluessel konsistent.
    """
    )

    mo.ui.tabs(
        {
            "Datenquellen": mo.vstack(
                [mo.md("## Woher die Daten kommen"), mo.ui.table(sources_df, selection=None)]
            ),
            "Prozess": process_md,
            "Transformationsbeispiele": mo.vstack(
                [
                    mo.md("## Konkrete Transformationsbeispiele"),
                    mo.ui.table(transform_examples_df, selection=None),
                ]
            ),
            "Entitaetsmodell": mo.vstack(
                [mo.md("## Finale Warehouse-Entitaeten"), mo.ui.table(entities_df, selection=None)]
            ),
            "SQL-Beispiel": sql_md,
        }
    )
    return


if __name__ == "__main__":
    app.run()
