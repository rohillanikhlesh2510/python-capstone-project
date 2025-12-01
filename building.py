
import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

# ----------------------
# FOLDER PATHS
# ----------------------
DATA_FOLDER = "data"
OUTPUT_FOLDER = "output"

Path(OUTPUT_FOLDER).mkdir(exist_ok=True)


# ------------------------------------------------------
# STEP 1 — READ & COMBINE ALL CSV FILES FROM DATA FOLDER
# ------------------------------------------------------
def load_all_data():
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
    all_dfs = []

    for file in files:
        path = os.path.join(DATA_FOLDER, file)
        df = pd.read_csv(path)

        # column normalization
        df.columns = [c.lower() for c in df.columns]

        # find timestamp column
        time_col = [c for c in df.columns if "time" in c or "date" in c][0]
        energy_col = [c for c in df.columns if "kwh" in c or "energy" in c][0]

        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df[energy_col] = pd.to_numeric(df[energy_col], errors="coerce")

        df = df.dropna()

        df = df.rename(columns={time_col: "timestamp", energy_col: "kwh"})

        building_name = file.replace(".csv", "")
        df["building"] = building_name

        df = df[["timestamp", "kwh", "building"]]
        df = df.sort_values("timestamp")

        all_dfs.append(df)

    if len(all_dfs) == 0:
        print("❌ No CSV files found!")
        return None

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df = final_df.set_index("timestamp")
    return final_df


# ------------------------------------------------------
# STEP 2 — AGGREGATION FUNCTIONS
# ------------------------------------------------------
def daily_data(df):
    return df.groupby("building")["kwh"].resample("D").sum().reset_index()


def weekly_data(df):
    return df.groupby("building")["kwh"].resample("W").sum().reset_index()


def building_summary(df):
    return (
        df.groupby("building")["kwh"]
        .agg(["sum", "mean", "max", "min", "median"])
        .reset_index()
    )


# ------------------------------------------------------
# STEP 3 — OOP CLASSES (Assignment Requirement)
# ------------------------------------------------------
class MeterReading:
    def __init__(self, timestamp, kwh):
        self.timestamp = timestamp
        self.kwh = kwh


class Building:
    def __init__(self, name):
        self.name = name
        self.readings = []

    def add_reading(self, ts, kwh):
        self.readings.append(MeterReading(ts, kwh))

    def total(self):
        return sum(r.kwh for r in self.readings)


class BuildingManager:
    def __init__(self):
        self.buildings = {}

    def load_df(self, df):
        for name, group in df.groupby("building"):
            b = Building(name)
            for ts, row in group.iterrows():
                b.add_reading(ts, row["kwh"])
            self.buildings[name] = b

    def report(self):
        return {b: self.buildings[b].total() for b in self.buildings}


# ------------------------------------------------------
# STEP 4 — VISUALIZATION
# ------------------------------------------------------
def make_dashboard(daily, weekly, df):
    plt.figure(figsize=(12, 15))

    # --- Daily Trend
    plt.subplot(3, 1, 1)
    for b, g in daily.groupby("building"):
        plt.plot(g["timestamp"], g["kwh"], label=b)
    plt.title("Daily Consumption Trend")
    plt.legend()

    # --- Weekly Bar Chart
    plt.subplot(3, 1, 2)
    weekly_avg = weekly.groupby("building")["kwh"].mean()
    plt.bar(weekly_avg.index, weekly_avg.values)
    plt.title("Weekly Average Usage")
    plt.xticks(rotation=45)

    # --- Peak Scatter
    plt.subplot(3, 1, 3)
    hourly = df.groupby("building")["kwh"].resample("H").sum().reset_index()
    top = hourly.sort_values("kwh", ascending=False).head(200)
    plt.scatter(top["timestamp"], top["kwh"], alpha=0.6)
    plt.title("Peak Hourly Consumption")

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/dashboard.png")
    print("✅ Dashboard saved: output/dashboard.png")


# ------------------------------------------------------
# STEP 5 — SAVE OUTPUT FILES
# ------------------------------------------------------
def save_results(df, daily, weekly, summary):
    df.reset_index().to_csv(f"{OUTPUT_FOLDER}/cleaned_data.csv", index=False)
    daily.to_csv(f"{OUTPUT_FOLDER}/daily.csv", index=False)
    weekly.to_csv(f"{OUTPUT_FOLDER}/weekly.csv", index=False)
    summary.to_csv(f"{OUTPUT_FOLDER}/summary.csv", index=False)

    total = summary["sum"].sum()
    top = summary.sort_values("sum", ascending=False).iloc[0]

    with open(f"{OUTPUT_FOLDER}/summary.txt", "w") as f:
        f.write(f"Total Campus Consumption: {total:.2f} kWh\n")
        f.write(f"Highest Building: {top['building']} ({top['sum']:.2f} kWh)\n")

    print("📄 Text summary saved.")


# ------------------------------------------------------
# MAIN SCRIPT
# ------------------------------------------------------
def main():
    print("Loading data...")
    df = load_all_data()

    if df is None:
        return

    daily = daily_data(df)
    weekly = weekly_data(df)
    summary = building_summary(df)

    manager = BuildingManager()
    manager.load_df(df)
    print("Manager Report:", manager.report())

    make_dashboard(daily, weekly, df)
    save_results(df, daily, weekly, summary)

    print("\n🎉 All tasks completed successfully!")


if __name__ == "__main__":
    main()
