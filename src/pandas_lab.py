import pandas as pd


def print_section(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main() -> None:
    # Load logs
    df = pd.read_csv("data/logs.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Basic derived fields
    df["hour"] = df["timestamp"].dt.hour
    df["after_hours"] = ((df["hour"] < 8) | (df["hour"] > 18)).astype(int)

    df["sensitive_flag"] = (df["file_sensitivity"] >= 2).astype(int)
    df["external_flag"] = (df["dest_domain"].astype(str).str.lower() == "external").astype(int)

    # Headline metrics (non-technical friendly)
    total_events = len(df)
    total_users = df["user_id"].nunique()
    total_bytes_out = int(df["bytes_out"].fillna(0).sum())
    after_hours_events = int(df["after_hours"].sum())
    sensitive_events = int(df["sensitive_flag"].sum())
    external_events = int(df["external_flag"].sum())
    external_bytes = int(df.loc[df["external_flag"] == 1, "bytes_out"].fillna(0).sum())

    print_section("UEBA + DLP Log Summary")
    print(f"Total activity events: {total_events}")
    print(f"Unique users observed: {total_users}")
    print(f"Total data transferred (bytes): {total_bytes_out:,}")
    print(f"External transfers: {external_events} events, {external_bytes:,} bytes")
    print(f"Sensitive data events: {sensitive_events}")
    print(f"After-hours activity events: {after_hours_events}")

    # User-level features (for you + later pipeline stages)
    user_features = (
        df.groupby("user_id")
        .agg(
            event_count=("event_type", "count"),
            unique_ips=("src_ip", "nunique"),
            total_bytes=("bytes_out", "sum"),
            external_transfers=("external_flag", "sum"),
            external_bytes=("bytes_out", lambda s: s[df.loc[s.index, "external_flag"] == 1].sum()),
            sensitive_events=("sensitive_flag", "sum"),
            after_hours_events=("after_hours", "sum"),
        )
        .reset_index()
    )

    # Simple “risk hint” score just for readable ranking (NOT your final risk engine)
    # Weighted toward external + sensitive + after-hours.
    user_features["risk_hint"] = (
        0.4 * user_features["external_transfers"]
        + 0.000001 * user_features["external_bytes"]
        + 0.4 * user_features["sensitive_events"]
        + 0.2 * user_features["after_hours_events"]
    )

    # Clean top table for non-technical readers
    top = (
        user_features.sort_values("risk_hint", ascending=False)
        .head(10)[
            [
                "user_id",
                "event_count",
                "external_transfers",
                "external_bytes",
                "sensitive_events",
                "after_hours_events",
            ]
        ]
        .copy()
    )

    # Make numbers pretty
    top["external_bytes"] = top["external_bytes"].fillna(0).astype(int)

    print_section("Top Users to Review (Risk Signals)")
    if len(top) == 0:
        print("No users found.")
    else:
        print(top.to_string(index=False))

    print_section("Plain-English Interpretation")
    print(
        "- External transfers + sensitive file activity + after-hours usage are common insider-risk signals.\n"
        "- The table above is a starting point to decide which users/sessions deserve deeper investigation.\n"
        "- Next we’ll move from user-level to session-level features (much stronger for UEBA)."
    )

    # Save outputs for pipeline
    user_features.to_csv("data/user_features.csv", index=False)
    print("\nSaved: data/user_features.csv")


if __name__ == "__main__":
    main()