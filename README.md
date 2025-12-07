Creative Performance Analytics – ETL Pipeline

Author: Sofiia Hakalo
Stack: Python · Pandas · NumPy · SQLAlchemy · MySQL · Tableau

Project Overview

This repository contains an end-to-end analytics workflow for evaluating the performance of marketing creatives.

📌 The solution includes:
	•	Automated ETL pipeline (Python) for cleaning and transforming four logical data sources:
Creative backlog, Facebook Ads data, Campaign–Adset mapping, Google Ad Manager revenue.
	•	Joining datasets into a unified fact_creative_performance table.
	•	Calculation of key performance metrics (ROI, CPC, Profit, Success Rate).
    •	Writing analytical outputs to MySQL.
	•	Exporting final aggregated datasets into /outputs for BI visualization (Tableau).
	•	Author-level aggregation used to identify top-performing creative authors.

📌 Key Metrics

Revenue: total_revenue = banner_revenue + video_revenue
Profit: profit = total_revenue – spend
ROI: roi = total_revenue / spend
CPC: cpc = spend / clicks
Success flag: is_profitable = total_revenue > spend

📌 ETL Steps

	1.	Load raw data from Excel export.
	2.	Normalize fields: dates, numeric formats, column names.
	3.	Parse campaign_name → articleid, author, media, version, type.
	4.	Join Facebook Ads → Mapping (campaign_id).
	5.	Join mapping → GAM revenue (adset_id + date).
	6.	Join with Creative backlog (creative attributes).
	7.	Compute metrics.
	8.	Save results to:
	•	MySQL database
	•	CSV files in the /outputs directory

📌 Database Output

MySQL writes two tables:
	•	fact_creative_performance
	•	author_performance_summary
Used as data sources for Tableau dashboards.

📌 Running the ETL

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/etl_pipeline.py