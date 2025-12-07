"""
Creative Analytics – ETL Pipeline
Author: Sofiia Hakalo

This script:
- loads data from Excel export of the provided Google Sheets
- cleans and normalizes each dataset
- joins the 4 logical sources (creative backlog, fb ads, mapping, GAM revenue)
- computes key metrics (ROI, profit, CPC, is_profitable)
- builds fact & aggregate tables
- writes results to MySQL and CSV
"""

from pathlib import Path
import re

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

DATA_PATH = Path("data/dataset.xlsx")
DB_URL = "mysql+pymysql://root:root1234@localhost/creative_analytics?unix_socket=/tmp/mysql.sock"


# LOAD RAW DATA

def load_data(path: Path):
    print(f"[LOAD] Reading Excel from {path} ...")
    creative = pd.read_excel(path, sheet_name="Creative backlog")
    fb = pd.read_excel(path, sheet_name="Facebook Ads data")
    gam = pd.read_excel(path, sheet_name="Google Ad Manager revenue data")
    mapping = pd.read_excel(path, sheet_name="Campaigns_Adsets")
    print(
        f"[LOAD] Shapes – creative: {creative.shape}, fb: {fb.shape}, "
        f"gam: {gam.shape}, mapping: {mapping.shape}"
    )
    return creative, fb, gam, mapping

# NORMALIZATION

def normalize_creative(df: pd.DataFrame) -> pd.DataFrame:
    print("[NORM] Normalizing Creative backlog...")
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"])

    if "articleid" in df.columns:
        df["articleid"] = df["articleid"].astype(str)

    for col in ["author", "type", "media", "version"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def normalize_fb(df: pd.DataFrame) -> pd.DataFrame:
    print("[NORM] Normalizing Facebook Ads data...")
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    for col in ["spend", "clicks"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # parse campaign_name -> articleid, type, version, author, media
    if "campaign_name" in df.columns:
        pattern = re.compile(
            r"(?P<articleid>\d+)\s+"
            r"(?P<type>\w+)\s+"
            r"(?P<version>v\d+)\s+"
            r"android\s+\((?P<author>[^)]+)\)\s+"
            r"(?P<media>\w+)",
            re.IGNORECASE,
        )
        parsed = df["campaign_name"].str.extract(pattern)
        parsed["articleid"] = parsed["articleid"].astype(str)

        for col in ["author", "type", "version", "media"]:
            parsed[col] = parsed[col].astype(str).str.strip()

        df = pd.concat([df, parsed], axis=1)

    return df


def normalize_gam(df: pd.DataFrame) -> pd.DataFrame:
    print("[NORM] Normalizing GAM revenue data...")
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    for col in ["banner_revenue", "video_revenue"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def normalize_mapping(df: pd.DataFrame) -> pd.DataFrame:
    print("[NORM] Normalizing Campaigns_Adsets mapping...")
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

# BUILD FACT TABLE

def build_fact_table(
    creative: pd.DataFrame,
    fb: pd.DataFrame,
    gam: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    print("[FACT] Joining Facebook data with mapping...")
    fb_map = fb.merge(mapping, on="campaign_id", how="left")

    print("[FACT] Joining with GAM revenue on [adset_id, date]...")
    fb_map_gam = fb_map.merge(
        gam,
        on=["adset_id", "date"],
        how="left",
    )

    print("[FACT] Joining with Creative backlog on [articleid, author, type, media, version]...")
    fact = fb_map_gam.merge(
        creative,
        on=["articleid", "author", "type", "media", "version"],
        how="left",
    )

    print("[FACT] Computing metrics...")
    for col in ["banner_revenue", "video_revenue"]:
        if col in fact.columns:
            fact[col] = fact[col].fillna(0)
        else:
            fact[col] = 0.0

    fact["total_revenue"] = fact["banner_revenue"] + fact["video_revenue"]
    fact["profit"] = fact["total_revenue"] - fact["spend"]

    fact["roi"] = np.where(
        fact["spend"] > 0,
        fact["total_revenue"] / fact["spend"],
        np.nan,
    )

    fact["cpc"] = np.where(
        fact["clicks"] > 0,
        fact["spend"] / fact["clicks"],
        np.nan,
    )

    fact["is_profitable"] = fact["total_revenue"] > fact["spend"]

    print(f"[FACT] Fact table shape: {fact.shape}")
    return fact


def aggregate_by_author(fact: pd.DataFrame) -> pd.DataFrame:
    print("[AGG] Aggregating by author...")
    agg = (
        fact.groupby("author", dropna=True)
        .agg(
            campaigns=("campaign_id", "nunique"),
            creatives=("headline", "nunique"),
            total_spend=("spend", "sum"),
            total_revenue=("total_revenue", "sum"),
            total_profit=("profit", "sum"),
            avg_roi=("roi", "mean"),
            success_rate=("is_profitable", "mean"),
        )
        .reset_index()
    )
    agg["success_rate"] = (agg["success_rate"] * 100).round(1)
    print(f"[AGG] Author summary shape: {agg.shape}")
    return agg

# SAVE TO MYSQL + CSV

def main():
    print("=== Creative Analytics – ETL started ===")

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {DATA_PATH}")

    creative, fb, gam, mapping = load_data(DATA_PATH)

    creative_n = normalize_creative(creative)
    fb_n = normalize_fb(fb)
    gam_n = normalize_gam(gam)
    mapping_n = normalize_mapping(mapping)

    fact = build_fact_table(creative_n, fb_n, gam_n, mapping_n)
    authors = aggregate_by_author(fact)

    print(f"[SAVE] Fact rows: {len(fact)}, authors: {len(authors)}")

    # Save to CSV
    OUTPUT_DIR = Path("outputs")
    OUTPUT_DIR.mkdir(exist_ok=True)

    fact.to_csv(OUTPUT_DIR / "fact_creative_performance.csv", index=False)
    authors.to_csv(OUTPUT_DIR / "author_performance_summary.csv", index=False)

    print("[SAVE] CSV files written to /outputs")

    # Save to MySQL
    print("[SAVE] Writing tables to MySQL...")
    engine = create_engine(DB_URL)

    fact.to_sql("fact_creative_performance", engine, if_exists="replace", index=False)
    authors.to_sql("author_performance_summary", engine, if_exists="replace", index=False)

    print("[DONE] ETL finished successfully.")


if __name__ == "__main__":
    main()