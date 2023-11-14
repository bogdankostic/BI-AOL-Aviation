import itertools

import pandas as pd
import pyexasol
import united_states

from ETL.config import EXASOL_HOST, EXASOL_PORT, EXASOL_USER, EXASOL_PASSWORD


def get_airports():
    # Create a connection to Exasol
    db_con = pyexasol.connect(
        dsn=f"{EXASOL_HOST}:{EXASOL_PORT}",
        user=EXASOL_USER,
        password=EXASOL_PASSWORD,
        compression=True,
        schema="AOL_SCHEMA",
        protocol_version=pyexasol.PROTOCOL_V1,
    )
    # Get airport dimension
    airports = db_con.export_to_list(
        "SELECT ORIGIN, DESTINATION FROM AOL_SCHEMA.FLIGHTS"
    )
    airports = set(itertools.chain.from_iterable(airports))

    return airports


if __name__ == "__main__":
    us = united_states.UnitedStates()
    airports = get_airports()
    df = pd.read_csv(
        "../../data/airports/airports.csv",
        usecols=[1, 2, 3, 4, 6, 7],
        names=["Name", "City", "Country", "IATA_Code", "Latitude", "Longitude"],
    )

    # Filter out airports that are not in the flights data
    df = df[df["IATA_Code"].isin(airports)]
    # Add State column based on latitude and longitude
    df["State"] = df.apply(
        lambda row: us.from_coords(row["Latitude"], row["Longitude"])[0].abbr, axis=1
    )
    # Reorder columns
    df = df[["IATA_Code", "Name", "City", "State", "Country", "Latitude", "Longitude"]]

    # Save as CSV file
    df.to_csv("../../data/airports/airports_processed.csv", index=False)
