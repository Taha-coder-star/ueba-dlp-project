import pandas as pd


SESSION_GAP_MINUTES = 30


def main():
   
    df = pd.read_csv("data/logs.csv")

    df["timestamp"] = pd.to_datetime(df["timestamp"])

   
    df = df.sort_values(["user_id", "timestamp"])

   
    df["time_diff"] = df.groupby("user_id")["timestamp"].diff()

   
    df["new_session"] = (
        df["time_diff"].dt.total_seconds().div(60) > SESSION_GAP_MINUTES
    )

   
    df["new_session"] = df["new_session"].fillna(True)

   
    df["session_id"] = df.groupby("user_id")["new_session"].cumsum()

   
    df.to_csv("data/sessions.csv", index=False)

    print("Sessionization complete.")
    print(df[["user_id", "timestamp", "session_id"]].head(20))


if __name__ == "__main__":
    main()