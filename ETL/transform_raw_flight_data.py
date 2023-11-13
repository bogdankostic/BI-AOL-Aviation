import glob

import pandas as pd
import pyexasol


MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
NUMBER_TO_MONTH_NAME = {v: k for k, v in MONTH_NAME_TO_NUMBER.items()}

EXASOL_HOST = "192.168.0.251"
EXASOL_PORT = "8563"
EXASOL_USER = "sys"
EXASOL_PASSWORD = "exasol"


def merge_and_update_time(df, time_dim, time_cols, id_col):
    merge_cols = ["year", "month", "day", "hour", "minute"]
    df = df.merge(time_dim, left_on=time_cols, right_on=merge_cols, how="left")
    df.rename(columns={"TIME_ID": id_col}, inplace=True)
    df.drop(columns=merge_cols, inplace=True)

    # Find rows in df where the time does not exist in the time dimension table
    mask_missing_times = df[id_col].isna()
    missing_times = df[mask_missing_times]

    # Add these times to the time dimension table
    new_times = missing_times[time_cols].drop_duplicates()
    new_times["TIME_ID"] = range(time_dim["TIME_ID"].max() + 1, time_dim["TIME_ID"].max() + 1 + len(new_times))
    new_times.rename(columns=dict(zip(time_cols, merge_cols)), inplace=True)
    time_dim = pd.concat([time_dim, new_times])

    # Merge again to get the new time IDs
    df.drop(columns=id_col, inplace=True)
    df = df.merge(time_dim, left_on=time_cols, right_on=merge_cols, how="left")
    df.rename(columns={"TIME_ID": id_col}, inplace=True)
    df.drop(columns=merge_cols, inplace=True)

    return df, time_dim, new_times


def get_time_dim():
    # Create a connection to Exasol
    db_con = pyexasol.connect(
        dsn=f"{EXASOL_HOST}:{EXASOL_PORT}",
        user=EXASOL_USER,
        password=EXASOL_PASSWORD,
        compression=True,
        schema="AOL_SCHEMA",
        protocol_version=pyexasol.PROTOCOL_V1,
    )
    # Get time dimension
    time_dim = db_con.export_to_pandas(
        "SELECT " 'ID as "TIME_ID", ' '"year", ' '"month", ' '"day", ' '"hour", ' '"minute" ' "FROM AOL_SCHEMA.TIMEDIM "
    )
    db_con.close()
    time_dim["month"] = time_dim["month"].map(lambda m: MONTH_NAME_TO_NUMBER[m.strip()])
    time_dim.drop_duplicates(subset=["year", "month", "day", "hour", "minute"], inplace=True, keep="first")

    return time_dim


if __name__ == "__main__":
    time_dim = get_time_dim()

    all_flights = pd.DataFrame(
        columns=[
            "Carrier",
            "Flight_Number",
            "Tail_Number",
            "Origin",
            "Destination",
            "Delay",
            "Delay_Carrier",
            "Delay_Weather",
            "Delay_NAS",
            "Delay_Security",
            "Delay_Late_Aircraft",
            "Scheduled_Time_ID",
            "Actual_Time_ID",
        ]
    )
    all_new_times = pd.DataFrame(columns=["TIME_ID", "year", "month", "day", "hour", "minute"])

    for filename in glob.glob("../data/raw/*.csv"):
        with open(filename) as file:
            print(f"Processing {filename}")
            file.readline()  # Skip the first line
            dest_line = file.readline()
            dest_airport = dest_line.split("(")[1][:-2]
            df = pd.read_csv(file, skiprows=4)

        # Write "CANCELED" in rows where tail number is missing
        df["Tail Number"].fillna("CANCELED", inplace=True)

        # Combine Dates and Times into a single column
        df.replace(to_replace="24:00", value="23:59", inplace=True)
        scheduled_times = pd.to_datetime(
            df["Date (MM/DD/YYYY)"] + " " + df["Scheduled Arrival Time"],
            format="%m/%d/%Y %H:%M",
        )
        actual_times = pd.to_datetime(
            df["Date (MM/DD/YYYY)"] + " " + df["Actual Arrival Time"],
            format="%m/%d/%Y %H:%M",
        )

        # Keep only flights from September 2005 to May 2006
        mask = (scheduled_times >= "2005-09-01") & (scheduled_times <= "2006-05-31")
        df = df.loc[mask]
        scheduled_times = scheduled_times.loc[mask]
        actual_times = actual_times.loc[mask]

        # Cast all numbers to integers
        df = df.astype(
            {
                "Flight Number": int,
                "Arrival Delay (Minutes)": int,
                "Delay Carrier (Minutes)": int,
                "Delay Weather (Minutes)": int,
                "Delay National Aviation System (Minutes)": int,
                "Delay Security (Minutes)": int,
                "Delay Late Aircraft Arrival (Minutes)": int,
            }
        )

        # Add data to the main dataframe
        df = pd.DataFrame(
            {
                "Carrier": df["Carrier Code"],
                "Flight_Number": df["Flight Number"],
                "Tail_Number": df["Tail Number"],
                "Origin": df["Origin Airport"],
                "Destination": [dest_airport] * df.shape[0],
                "Scheduled_Arrival": scheduled_times,
                "Actual_Arrival": actual_times,
                "Delay": df["Arrival Delay (Minutes)"],
                "Delay_Carrier": df["Delay Carrier (Minutes)"],
                "Delay_Weather": df["Delay Weather (Minutes)"],
                "Delay_NAS": df["Delay National Aviation System (Minutes)"],
                "Delay_Security": df["Delay Security (Minutes)"],
                "Delay_Late_Aircraft": df["Delay Late Aircraft Arrival (Minutes)"],
            }
        )

        df["Scheduled_Arrival_Year"] = df["Scheduled_Arrival"].dt.year
        df["Scheduled_Arrival_Month"] = df["Scheduled_Arrival"].dt.month
        df["Scheduled_Arrival_Day"] = df["Scheduled_Arrival"].dt.day
        df["Scheduled_Arrival_Hour"] = df["Scheduled_Arrival"].dt.hour
        df["Scheduled_Arrival_Minute"] = df["Scheduled_Arrival"].dt.minute

        df["Actual_Arrival_Year"] = df["Actual_Arrival"].dt.year
        df["Actual_Arrival_Month"] = df["Actual_Arrival"].dt.month
        df["Actual_Arrival_Day"] = df["Actual_Arrival"].dt.day
        df["Actual_Arrival_Hour"] = df["Actual_Arrival"].dt.hour
        df["Actual_Arrival_Minute"] = df["Actual_Arrival"].dt.minute

        time_cols_scheduled = [
            "Scheduled_Arrival_Year",
            "Scheduled_Arrival_Month",
            "Scheduled_Arrival_Day",
            "Scheduled_Arrival_Hour",
            "Scheduled_Arrival_Minute",
        ]
        time_cols_actual = [
            "Actual_Arrival_Year",
            "Actual_Arrival_Month",
            "Actual_Arrival_Day",
            "Actual_Arrival_Hour",
            "Actual_Arrival_Minute",
        ]

        # Process Scheduled Times
        df, time_dim, new_times = merge_and_update_time(df, time_dim, time_cols_scheduled, "Scheduled_Time_ID")
        all_new_times = pd.concat([all_new_times, new_times], ignore_index=True)

        # Process Actual Times for non-canceled flights
        canceled_mask = df["Tail_Number"] == "CANCELED"
        non_canceled_flights = df[~canceled_mask]
        non_canceled_flights, time_dim, new_times = merge_and_update_time(
            non_canceled_flights, time_dim, time_cols_actual, "Actual_Time_ID"
        )
        all_new_times = pd.concat([all_new_times, new_times], ignore_index=True)
        # Set the actual time ID to None for canceled flights
        if any(canceled_mask):
            df.loc[canceled_mask, "Actual_Time_ID"] = None
        # Combine the canceled flights with the non-canceled flights
        df.loc[~canceled_mask, "Actual_Time_ID"] = non_canceled_flights["Actual_Time_ID"].tolist()

        df.drop(
            ["Scheduled_Arrival", "Actual_Arrival"] + time_cols_actual + time_cols_scheduled,
            inplace=True,
            axis=1,
        )
        all_flights = pd.concat([all_flights, df], ignore_index=True)

    # Bring data in right format
    all_new_times["month"] = all_new_times["month"].map(lambda m: NUMBER_TO_MONTH_NAME[m])
    all_new_times["day"] = all_new_times["day"].map(lambda d: str(d).zfill(2))
    all_new_times["hour"] = all_new_times["hour"].map(lambda h: str(h).zfill(2))
    all_new_times["minute"] = all_new_times["minute"].map(lambda m: str(m).zfill(2))

    # Save data to CSV
    all_flights.to_csv("data/flights.csv", index=False)
    all_new_times.to_csv("data/new_times.csv", index=False)
