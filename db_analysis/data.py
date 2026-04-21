import argparse
import os
import sys

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, text

# Query for average approved trip duration by country
AVG_TRIP_DURATION_QUERY = """
SELECT
	c.country,
	ROUND(AVG(EXTRACT(EPOCH FROM (t."end" - t.start)) / 60), 1) AS avg_minutes
FROM public.trip t
JOIN public.community c ON t.community_id = c.id
WHERE t.status = 'approved' AND t."end" IS NOT NULL
GROUP BY c.country
ORDER BY avg_minutes DESC;
""".strip()

# Query for total number of chapters in the United States
CHAPTERS_PER_COUNTRY_QUERY = """
SELECT COUNT(*) AS us_chapter_count
FROM public.community
WHERE LOWER(country) IN ('us', 'usa', 'united states', 'united states of america');
""".strip()


# Query for active chapters by country with 2+ approved rides in 2025
ACTIVE_CHAPTERS_PER_COUNTRY_QUERY = """
WITH active_chapters AS (
SELECT
	c.id,
	c.country,
	COUNT(t.id) AS ride_count
FROM public.community c
JOIN public.trip t ON t.community_id = c.id
WHERE t.status = 'approved'
	AND EXTRACT(YEAR FROM t.start) = 2025
GROUP BY c.id, c.country
HAVING COUNT(t.id) >= 2
)
SELECT
c.country,
	COUNT(*) AS active_chapter_count
FROM active_chapters c
GROUP BY c.country
ORDER BY active_chapter_count DESC, c.country;
""".strip()

# Query for total chapters per country as of the end of 2025
TOTAL_CHAPTERS_PER_COUNTRY_QUERY = """
SELECT
	c.country,
	COUNT(*) AS total_chapter_count
FROM public.community c
WHERE c.created_at IS NOT NULL
	AND c.created_at < DATE '2026-01-01'
GROUP BY c.country
ORDER BY total_chapter_count DESC, c.country;
""".strip()

USER_EMAILS_QUERY = """
SELECT
	DISTINCT u.email
FROM public."user" u
WHERE u.email IS NOT NULL
ORDER BY u.email;

""".strip()

# Query for inactive pilots grouped by total approved rides completed
# Inactive means the pilot's last approved trip was at least 1 year ago from today
PILOTS_INACTIVE_QUERY = """
WITH pilot_last_approved AS (
	SELECT
		t.pilot_id,
		MAX(t.start) AS last_approved_trip_at,
		COUNT(*) AS approved_trip_count
	FROM public.trip t
	WHERE t.status = 'approved'
		AND t.pilot_id IS NOT NULL
	GROUP BY t.pilot_id
)
SELECT
	pla.approved_trip_count AS rides_completed,
	COUNT(*) AS inactive_pilot_count
FROM pilot_last_approved pla
WHERE pla.last_approved_trip_at <= NOW() - INTERVAL '1 year'
GROUP BY pla.approved_trip_count
ORDER BY rides_completed ASC;
""".strip()

# Query for number of trips per pilot, inlcuding pilots with zero trips
# Counts approved rides up to today's date and sorts by number of pilots with that trip count, descending
PILOT_TRIP_COUNTS_QUERY = """
WITH pilot_list AS (
	SELECT
		u.id AS pilot_id
	FROM public.account a
	JOIN public."user" u ON u.id = a.user_id
	WHERE a.is_trained_pilot = true
),
trip_counts AS (
	SELECT
		t.pilot_id,
		COUNT(*) AS trip_count
	FROM public.trip t
	WHERE t.status = 'approved'
		AND t.start < (CURRENT_DATE + INTERVAL '1 day')
		AND t.pilot_id IS NOT NULL
	GROUP BY t.pilot_id
)
SELECT
	COALESCE(tc.trip_count, 0) AS rides_completed,
	COUNT(*) AS pilot_count
FROM pilot_list pl
LEFT JOIN trip_counts tc ON tc.pilot_id = pl.pilot_id
GROUP BY COALESCE(tc.trip_count, 0)
ORDER BY pilot_count DESC, rides_completed ASC;
""".strip()


# Query for pilot details filtered by an exact number of approved rides completed
PILOT_DETAILS_BY_RIDE_COUNT_QUERY = """
WITH pilot_ride_summary AS (
	SELECT
		t.pilot_id,
		COUNT(*) AS rides_completed,
		MIN(t.start) AS first_ride_date,
		MAX(t.start) AS last_ride_date
	FROM public.trip t
	WHERE t.status = 'approved'
		AND t.pilot_id IS NOT NULL
	GROUP BY t.pilot_id
),
last_ride_location AS (
	SELECT DISTINCT ON (t.pilot_id)
		t.pilot_id,
		c.name AS location,
		c.country AS country
	FROM public.trip t
	JOIN public.community c ON c.id = t.community_id
	WHERE t.status = 'approved'
		AND t.pilot_id IS NOT NULL
	ORDER BY t.pilot_id, t.start DESC
)
SELECT
	u.first_name,
	u.last_name,
	COALESCE(lrl.location, 'Unknown') AS location,
	COALESCE(lrl.country, NULLIF(u.country, ''), 'Unknown') AS country,
	prs.rides_completed,
	prs.first_ride_date,
	prs.last_ride_date
FROM pilot_ride_summary prs
JOIN public."user" u ON u.id = prs.pilot_id
LEFT JOIN last_ride_location lrl ON lrl.pilot_id = prs.pilot_id
WHERE prs.rides_completed = :ride_count
ORDER BY prs.last_ride_date DESC;
""".strip()

# Query for active chapters in denmark by year
# active chapters have 2 plus rides within the year
ACTIVE_CHAPTERS_IN_DENMARK_BY_YEAR_QUERY = """
WITH chapter_year_rides AS (
SELECT
	c.country,
	t.community_id,
	EXTRACT(YEAR FROM t.start) AS year,
	COUNT(*) AS ride_count
FROM public.community c
JOIN public.trip t ON t.community_id = c.id
WHERE c.country = 'Denmark'
	AND t.status = 'approved'
	AND t.start IS NOT NULL
	GROUP BY c.country, t.community_id, EXTRACT(YEAR FROM t.start)
	HAVING COUNT(*) >= 2
)
SELECT
	country,
	year,
	COUNT(*) AS active_chapter_count
FROM chapter_year_rides
GROUP BY country, year
ORDER BY year;
""".strip()

# Query for world chapter status by year (aggregated across all countries)
# Active: 2+ approved rides in the year
# Inactive with approved history: <2 in the year and has approved rides up to that year
# Inactive without rides: no approved rides up to that year
WORLD_CHAPTER_STATUS_BY_YEAR_QUERY = """
WITH world_chapters AS (
SELECT
	c.id AS community_id,
	c.country,
	c.created_at
FROM public.community c
WHERE c.created_at IS NOT NULL
),
year_bounds AS (
SELECT generate_series(
	(SELECT EXTRACT(YEAR FROM MIN(dc.created_at))::int FROM world_chapters dc),
	(SELECT LEAST(2025, GREATEST(
		EXTRACT(YEAR FROM MAX(dc.created_at))::int,
		COALESCE(
			(SELECT EXTRACT(YEAR FROM MAX(t.start))::int
			FROM public.trip t
			JOIN world_chapters dc2 ON dc2.community_id = t.community_id
			WHERE t.status = 'approved'
				AND t.start IS NOT NULL),
			EXTRACT(YEAR FROM MAX(dc.created_at))::int
		)
	)) FROM world_chapters dc)
)::int AS year
),
chapter_year AS (
SELECT
	dc.community_id,
	dc.country,
	y.year
FROM world_chapters dc
JOIN year_bounds y ON EXTRACT(YEAR FROM dc.created_at)::int <= y.year
),
approved_rides_by_year AS (
SELECT
	t.community_id,
	EXTRACT(YEAR FROM t.start)::int AS year,
	COUNT(*) AS approved_rides_in_year
FROM public.trip t
JOIN world_chapters dc ON dc.community_id = t.community_id
WHERE t.status = 'approved'
	AND t.start IS NOT NULL
GROUP BY t.community_id, EXTRACT(YEAR FROM t.start)::int
),
approved_history AS (
SELECT
	cy.community_id,
	cy.year,
	COALESCE(SUM(ary.approved_rides_in_year) FILTER (WHERE ary.year <= cy.year), 0) AS approved_rides_up_to_year
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary ON ary.community_id = cy.community_id
GROUP BY cy.community_id, cy.year
)
SELECT
	cy.year,
	COUNT(*) FILTER (WHERE COALESCE(ary.approved_rides_in_year, 0) >= 2) AS active_chapter_count,
	COUNT(*) FILTER (
		WHERE COALESCE(ary.approved_rides_in_year, 0) < 2
			AND ah.approved_rides_up_to_year > 0
	) AS inactive_with_approved_history_count,
	COUNT(*) FILTER (WHERE ah.approved_rides_up_to_year = 0) AS inactive_without_rides_count,
	COUNT(*) AS total_chapters_joined
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary
	ON ary.community_id = cy.community_id
	AND ary.year = cy.year
JOIN approved_history ah
	ON ah.community_id = cy.community_id
	AND ah.year = cy.year
GROUP BY cy.year
ORDER BY cy.year;
""".strip()

# Query for world chapter status by country and year
# Active: 2+ approved rides in the year
# Inactive with approved history: <2 in the year and has approved rides up to that year
# Inactive without rides: no approved rides up to that year
WORLD_CHAPTER_STATUS_BY_COUNTRY_YEAR_QUERY = """
WITH world_chapters AS (
SELECT
	c.id AS community_id,
	c.country,
	c.created_at
FROM public.community c
WHERE c.created_at IS NOT NULL
),
year_bounds AS (
SELECT generate_series(
	(SELECT EXTRACT(YEAR FROM MIN(dc.created_at))::int FROM world_chapters dc),
	(SELECT LEAST(2025, GREATEST(
		EXTRACT(YEAR FROM MAX(dc.created_at))::int,
		COALESCE(
			(SELECT EXTRACT(YEAR FROM MAX(t.start))::int
			FROM public.trip t
			JOIN world_chapters dc2 ON dc2.community_id = t.community_id
			WHERE t.status = 'approved'
				AND t.start IS NOT NULL),
			EXTRACT(YEAR FROM MAX(dc.created_at))::int
		)
	)) FROM world_chapters dc)
)::int AS year
),
chapter_year AS (
SELECT
	dc.community_id,
	dc.country,
	y.year
FROM world_chapters dc
JOIN year_bounds y ON EXTRACT(YEAR FROM dc.created_at)::int <= y.year
),
approved_rides_by_year AS (
SELECT
	t.community_id,
	EXTRACT(YEAR FROM t.start)::int AS year,
	COUNT(*) AS approved_rides_in_year
FROM public.trip t
JOIN world_chapters dc ON dc.community_id = t.community_id
WHERE t.status = 'approved'
	AND t.start IS NOT NULL
GROUP BY t.community_id, EXTRACT(YEAR FROM t.start)::int
),
approved_history AS (
SELECT
	cy.community_id,
	cy.year,
	COALESCE(SUM(ary.approved_rides_in_year) FILTER (WHERE ary.year <= cy.year), 0) AS approved_rides_up_to_year
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary ON ary.community_id = cy.community_id
GROUP BY cy.community_id, cy.year
)
SELECT
	cy.country,
	cy.year,
	COUNT(*) FILTER (WHERE COALESCE(ary.approved_rides_in_year, 0) >= 2) AS active_chapter_count,
	COUNT(*) FILTER (
		WHERE COALESCE(ary.approved_rides_in_year, 0) < 2
			AND ah.approved_rides_up_to_year > 0
	) AS inactive_with_approved_history_count,
	COUNT(*) FILTER (WHERE ah.approved_rides_up_to_year = 0) AS inactive_without_rides_count,
	COUNT(*) AS total_chapters_joined
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary
	ON ary.community_id = cy.community_id
	AND ary.year = cy.year
JOIN approved_history ah
	ON ah.community_id = cy.community_id
	AND ah.year = cy.year
GROUP BY cy.country, cy.year
ORDER BY cy.country, cy.year;
""".strip()

# Query for Denmark chapter status by year
# Active: 2+ approved rides in the year
# Inactive with approved history: <2 in the year and has approved rides up to that year
# Inactive without rides: no approved rides up to that year
DENMARK_CHAPTER_STATUS_BY_YEAR_QUERY = """
WITH denmark_chapters AS (
SELECT
	c.id AS community_id,
	c.created_at
FROM public.community c
WHERE c.country = 'Denmark'
	AND c.created_at IS NOT NULL
),
year_bounds AS (
SELECT generate_series(
	(SELECT EXTRACT(YEAR FROM MIN(dc.created_at))::int FROM denmark_chapters dc),
	(SELECT LEAST(2025, GREATEST(
		EXTRACT(YEAR FROM MAX(dc.created_at))::int,
		COALESCE(
			(SELECT EXTRACT(YEAR FROM MAX(t.start))::int
			FROM public.trip t
			JOIN denmark_chapters dc2 ON dc2.community_id = t.community_id
			WHERE t.status = 'approved'
				AND t.start IS NOT NULL),
			EXTRACT(YEAR FROM MAX(dc.created_at))::int
		)
	)) FROM denmark_chapters dc)
)::int AS year
),
chapter_year AS (
SELECT
	dc.community_id,
	y.year
FROM denmark_chapters dc
JOIN year_bounds y ON EXTRACT(YEAR FROM dc.created_at)::int <= y.year
),
approved_rides_by_year AS (
SELECT
	t.community_id,
	EXTRACT(YEAR FROM t.start)::int AS year,
	COUNT(*) AS approved_rides_in_year
FROM public.trip t
JOIN denmark_chapters dc ON dc.community_id = t.community_id
WHERE t.status = 'approved'
	AND t.start IS NOT NULL
GROUP BY t.community_id, EXTRACT(YEAR FROM t.start)::int
),
approved_history AS (
SELECT
	cy.community_id,
	cy.year,
	COALESCE(SUM(ary.approved_rides_in_year) FILTER (WHERE ary.year <= cy.year), 0) AS approved_rides_up_to_year
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary ON ary.community_id = cy.community_id
GROUP BY cy.community_id, cy.year
)
SELECT
	cy.year,
	COUNT(*) FILTER (WHERE COALESCE(ary.approved_rides_in_year, 0) >= 2) AS active_chapter_count,
	COUNT(*) FILTER (
		WHERE COALESCE(ary.approved_rides_in_year, 0) < 2
			AND ah.approved_rides_up_to_year > 0
	) AS inactive_with_approved_history_count,
	COUNT(*) FILTER (WHERE ah.approved_rides_up_to_year = 0) AS inactive_without_rides_count,
	COUNT(*) AS total_chapters_joined
FROM chapter_year cy
LEFT JOIN approved_rides_by_year ary
	ON ary.community_id = cy.community_id
	AND ary.year = cy.year
JOIN approved_history ah
	ON ah.community_id = cy.community_id
	AND ah.year = cy.year
GROUP BY cy.year
ORDER BY cy.year;
""".strip()



def get_db_config() -> dict:
	return {
		"host": os.getenv("PGHOST", "localhost"),
		"port": int(os.getenv("PGPORT", "5432")),
		"dbname": os.getenv("PGDATABASE", "book2go"),
		"user": os.getenv("PGUSER", "postgres"),
		"password": os.getenv("PGPASSWORD", ""),
		"connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
	}


def create_db_engine():
	config = get_db_config()
	password = config["password"].replace("@", "%40")
	url = (
		f"postgresql+psycopg2://{config['user']}:{password}@{config['host']}:{config['port']}/{config['dbname']}"
	)
	return create_engine(url, connect_args={"connect_timeout": config["connect_timeout"]})


def query_to_dataframe(engine, query: str, params: dict | None = None) -> pd.DataFrame:
	with engine.connect() as conn:
		return pd.read_sql(text(query), conn, params=params)


def export_dataframe(df: pd.DataFrame, export_name: str, export: bool) -> None:
	if not export:
		return

	os.makedirs("data_export", exist_ok=True)
	output_path = os.path.join("data_export", f"{export_name}.csv")
	df.to_csv(output_path, index=False)
	print(f"Exported {len(df)} rows to {output_path}")


def plot_avg_trip_duration_by_country(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, AVG_TRIP_DURATION_QUERY)
	if df.empty:
		print("No data returned for avg trip duration query.")
		return

	export_dataframe(df, "avg_duration", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(11, 6))
	plt.bar(df["country"], df["avg_minutes"])
	plt.title("Average Approved Trip Duration by Country")
	plt.xlabel("Country")
	plt.ylabel("Avg Minutes")
	plt.xticks(rotation=45, ha="right")
	plt.tight_layout()
	plt.show()


def show_pilot_details_by_ride_count(engine, ride_count: int, export: bool = False) -> None:
	df = query_to_dataframe(engine, PILOT_DETAILS_BY_RIDE_COUNT_QUERY, params={"ride_count": ride_count})
	if df.empty:
		print(f"No pilots found with exactly {ride_count} approved rides.")
		return

	export_dataframe(df, "pilot_details_by_ride_count", export)
	print(df.to_string(index=False))


def plot_active_chapters_in_denmark_by_year(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, ACTIVE_CHAPTERS_IN_DENMARK_BY_YEAR_QUERY)
	if df.empty:
		print("No Denmark active chapter data found.")
		return

	export_dataframe(df, "active_chapters_in_denmark_by_year", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(10, 6))
	bars = plt.bar(df["year"].astype(int).astype(str), df["active_chapter_count"])
	plt.title("Active Chapters in Denmark by Year")
	plt.xlabel("Year")
	plt.ylabel("Active Chapter Count")
	plt.xticks(rotation=45, ha="right")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def plot_world_chapters_by_year(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, WORLD_CHAPTER_STATUS_BY_YEAR_QUERY)

	if df.empty:
		print("No world chapter status data found.")
		return

	df["year"] = df["year"].astype(int)
	df["active_chapter_count"] = df["active_chapter_count"].fillna(0).astype(int)
	df["inactive_with_approved_history_count"] = df["inactive_with_approved_history_count"].fillna(0).astype(int)
	df["inactive_without_rides_count"] = df["inactive_without_rides_count"].fillna(0).astype(int)
	df["total_chapters_joined"] = df["total_chapters_joined"].fillna(0).astype(int)
	df = df.sort_values("year")

	export_dataframe(df, "world_chapters_by_year", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(12, 6))
	x_labels = df["year"].astype(str).tolist()
	active_counts = df["active_chapter_count"].tolist()
	inactive_with_history_counts = df["inactive_with_approved_history_count"].tolist()
	inactive_without_rides_counts = df["inactive_without_rides_count"].tolist()
	
	bars_active = plt.bar(x_labels, active_counts, label="Active Chapters", color="#FF0000")
	bars_inactive_with_history = plt.bar(
		x_labels,
		inactive_with_history_counts,
		bottom=active_counts,
		label="Inactive Chapters (Approved Ride History)",
		color="#FF8080",
	)
	bars_inactive_without_rides = plt.bar(
		x_labels,
		inactive_without_rides_counts,
		bottom=[
			active_counts[i] + inactive_with_history_counts[i]
			for i in range(len(active_counts))
		],
		label="Inactive Chapters (No Approved Rides)",
		color="#FFB3BA",
	)

	plt.title("World Chapters by Year: Active and Inactive Breakdown")
	plt.xlabel("Year")
	plt.ylabel("Chapter Count")
	plt.xticks(rotation=45, ha="right")
	plt.legend()

	for i, bar in enumerate(bars_active):
		active_height = int(bar.get_height())
		inactive_with_history_height = int(bars_inactive_with_history[i].get_height())
		inactive_without_rides_height = int(bars_inactive_without_rides[i].get_height())
		total = int(df.iloc[i]["total_chapters_joined"])
		
		if active_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height / 2,
				f"{active_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="white",
			)
		if inactive_with_history_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height + (inactive_with_history_height / 2),
				f"{inactive_with_history_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="white",
			)
		if inactive_without_rides_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height + inactive_with_history_height + (inactive_without_rides_height / 2),
				f"{inactive_without_rides_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="black",
			)
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			total + 0.5,
			f"{total}",
			ha="center",
			va="bottom",
			fontsize=9,
			fontweight="bold",
		)

	plt.tight_layout()
	plt.show()


def plot_world_active_vs_joined_by_year(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, WORLD_CHAPTER_STATUS_BY_COUNTRY_YEAR_QUERY)

	if df.empty:
		print("No world chapter status data found.")
		return

	df["year"] = df["year"].astype(int)
	df["active_chapter_count"] = df["active_chapter_count"].fillna(0).astype(int)
	df["inactive_with_approved_history_count"] = df["inactive_with_approved_history_count"].fillna(0).astype(int)
	df["inactive_without_rides_count"] = df["inactive_without_rides_count"].fillna(0).astype(int)
	df["total_chapters_joined"] = df["total_chapters_joined"].fillna(0).astype(int)

	export_dataframe(df, "world_active_vs_joined_by_year", export)

	# Get top countries by total chapters
	country_totals = df.groupby("country")["total_chapters_joined"].sum().nlargest(12).index.tolist()
	filtered_df = df[df["country"].isin(country_totals)].sort_values(["country", "year"])

	print(f"Top 12 countries by total chapters: {country_totals}")
	print(filtered_df.to_string(index=False))

	# Create subplots for top countries
	num_countries = len(country_totals)
	cols = 4
	rows = (num_countries + cols - 1) // cols

	fig, axes = plt.subplots(rows, cols, figsize=(16, 4 * rows))
	axes = axes.flatten()

	for idx, country in enumerate(country_totals):
		ax = axes[idx]
		country_data = filtered_df[filtered_df["country"] == country].sort_values("year")

		if country_data.empty:
			ax.set_title(f"{country} - No Data")
			continue

		x_labels = country_data["year"].astype(str).tolist()
		active_counts = country_data["active_chapter_count"].tolist()
		inactive_with_history_counts = country_data["inactive_with_approved_history_count"].tolist()
		inactive_without_rides_counts = country_data["inactive_without_rides_count"].tolist()

		bars_active = ax.bar(x_labels, active_counts, label="Active", color="#FF0000")
		bars_inactive_with_history = ax.bar(
			x_labels,
			inactive_with_history_counts,
			bottom=active_counts,
			label="Inactive (History)",
			color="#FF8080",
		)
		bars_inactive_without_rides = ax.bar(
			x_labels,
			inactive_without_rides_counts,
			bottom=[
				active_counts[i] + inactive_with_history_counts[i]
				for i in range(len(active_counts))
			],
			label="Inactive (No Rides)",
			color="#FFB3BA",
		)

		ax.set_title(f"{country}")
		ax.set_xlabel("Year")
		ax.set_ylabel("Chapter Count")
		ax.tick_params(axis="x", rotation=45)

		# Add data labels
		for i, bar in enumerate(bars_active):
			active_height = int(bar.get_height())
			inactive_with_history_height = int(bars_inactive_with_history[i].get_height())
			inactive_without_rides_height = int(bars_inactive_without_rides[i].get_height())
			total = int(country_data.iloc[i]["total_chapters_joined"])

			if active_height > 0:
				ax.text(
					bar.get_x() + bar.get_width() / 2,
					active_height / 2,
					f"{active_height}",
					ha="center",
					va="center",
					fontsize=7,
					fontweight="bold",
					color="white",
				)
			if inactive_with_history_height > 0:
				ax.text(
					bar.get_x() + bar.get_width() / 2,
					active_height + (inactive_with_history_height / 2),
					f"{inactive_with_history_height}",
					ha="center",
					va="center",
					fontsize=7,
					fontweight="bold",
					color="white",
				)
			if inactive_without_rides_height > 0:
				ax.text(
					bar.get_x() + bar.get_width() / 2,
					active_height + inactive_with_history_height + (inactive_without_rides_height / 2),
					f"{inactive_without_rides_height}",
					ha="center",
					va="center",
					fontsize=7,
					fontweight="bold",
					color="black",
				)
			ax.text(
				bar.get_x() + bar.get_width() / 2,
				total + 0.3,
				f"{total}",
				ha="center",
				va="bottom",
				fontsize=7,
				fontweight="bold",
			)

	# Hide extra subplots
	for idx in range(num_countries, len(axes)):
		axes[idx].set_visible(False)

	fig.suptitle("World Chapters by Country: Active and Inactive Breakdown", fontsize=14, fontweight="bold")
	fig.legend(["Active Chapters", "Inactive (Approved History)", "Inactive (No Approved Rides)"], loc="upper right")
	plt.tight_layout()
	plt.show()


def plot_denmark_active_vs_joined_by_year(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, DENMARK_CHAPTER_STATUS_BY_YEAR_QUERY)

	if df.empty:
		print("No Denmark chapter status data found.")
		return

	df["year"] = df["year"].astype(int)
	df["active_chapter_count"] = df["active_chapter_count"].fillna(0).astype(int)
	df["inactive_with_approved_history_count"] = df["inactive_with_approved_history_count"].fillna(0).astype(int)
	df["inactive_without_rides_count"] = df["inactive_without_rides_count"].fillna(0).astype(int)
	df["total_chapters_joined"] = df["total_chapters_joined"].fillna(0).astype(int)
	df = df.sort_values("year")

	export_dataframe(df, "denmark_active_vs_joined_by_year", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(12, 6))
	x_labels = df["year"].astype(str).tolist()
	active_counts = df["active_chapter_count"].tolist()
	inactive_with_history_counts = df["inactive_with_approved_history_count"].tolist()
	inactive_without_rides_counts = df["inactive_without_rides_count"].tolist()
	bars_active = plt.bar(x_labels, active_counts, label="Active Chapters", color="#FF0000")
	bars_inactive_with_history = plt.bar(
		x_labels,
		inactive_with_history_counts,
		bottom=active_counts,
		label="Inactive Chapters (Approved Ride History)",
		color="#FF8080",
	)
	bars_inactive_without_rides = plt.bar(
		x_labels,
		inactive_without_rides_counts,
		bottom=[
			active_counts[i] + inactive_with_history_counts[i]
			for i in range(len(active_counts))
		],
		label="Inactive Chapters (No Approved Rides)",
		color="#FFB3BA",
	)

	plt.title("Denmark Chapters by Year: Active and Inactive Breakdown")
	plt.xlabel("Year")
	plt.ylabel("Chapter Count")
	plt.xticks(rotation=45, ha="right")
	plt.legend()

	for i, bar in enumerate(bars_active):
		active_height = int(bar.get_height())
		inactive_with_history_height = int(bars_inactive_with_history[i].get_height())
		inactive_without_rides_height = int(bars_inactive_without_rides[i].get_height())
		total = int(df.iloc[i]["total_chapters_joined"])
		if active_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height / 2,
				f"{active_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="white",
			)
		if inactive_with_history_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height + (inactive_with_history_height / 2),
				f"{inactive_with_history_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="white",
			)
		if inactive_without_rides_height > 0:
			plt.text(
				bar.get_x() + bar.get_width() / 2,
				active_height + inactive_with_history_height + (inactive_without_rides_height / 2),
				f"{inactive_without_rides_height}",
				ha="center",
				va="center",
				fontsize=9,
				fontweight="bold",
				color="black",
			)
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			total + 0.5,
			f"{total}",
			ha="center",
			va="bottom",
			fontsize=9,
			fontweight="bold",
		)

	plt.tight_layout()
	plt.show()


def plot_us_chapter_count(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, CHAPTERS_PER_COUNTRY_QUERY)
	if df.empty:
		print("No data returned for US chapter count query.")
		return

	export_dataframe(df, "us_chapter_count", export)
	print(df.to_string(index=False))

	count = int(df.loc[0, "us_chapter_count"])
	plt.figure(figsize=(6, 4))
	bars = plt.bar(["United States"], [count])
	plt.title("Number of Chapters in the United States")
	plt.ylabel("Chapter Count")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def plot_active_us_chapters(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, ACTIVE_CHAPTERS_PER_COUNTRY_QUERY)
	if df.empty:
		print("No active chapters found with 2 or more rides in the past year.")
		return

	export_dataframe(df, "active_us_chapters", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(12, 6))
	bars = plt.bar(df["country"], df["active_chapter_count"])
	plt.title("Active Chapters by Country (2+ Approved Rides in Past Year)")
	plt.xlabel("Country")
	plt.ylabel("Active Chapter Count")
	plt.xticks(rotation=45, ha="right")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def plot_total_chapters_by_country(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, TOTAL_CHAPTERS_PER_COUNTRY_QUERY)
	if df.empty:
		print("No chapters found.")
		return

	export_dataframe(df, "total_chapters_by_country", export)
	print(df.to_string(index=False))

	plt.figure(figsize=(12, 6))
	bars = plt.bar(df["country"], df["total_chapter_count"])
	plt.title("Total Chapters by Country")
	plt.xlabel("Country")
	plt.ylabel("Total Chapter Count")
	plt.xticks(rotation=45, ha="right")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def plot_chapters_stacked_by_country(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, WORLD_CHAPTER_STATUS_BY_COUNTRY_YEAR_QUERY)

	if df.empty:
		print("No data available for stacked chapter plot.")
		return

	# Filter for 2025 only and exclude "delete" country (case-insensitive)
	df_2025 = df[(df["year"] == 2025) & (df["country"].str.lower() != "delete")].copy()
	df_2025["active_chapter_count"] = df_2025["active_chapter_count"].fillna(0).astype(int)
	df_2025["inactive_with_approved_history_count"] = df_2025["inactive_with_approved_history_count"].fillna(0).astype(int)
	df_2025["inactive_without_rides_count"] = df_2025["inactive_without_rides_count"].fillna(0).astype(int)
	df_2025["total_chapters_joined"] = df_2025["total_chapters_joined"].fillna(0).astype(int)
	df_2025 = df_2025.sort_values("total_chapters_joined", ascending=False)

	export_dataframe(df_2025, "chapters_stacked_by_country", export)
	print(df_2025.to_string(index=False))

	plt.figure(figsize=(14, 7))
	bar_width = 0.6
	x_pos = range(len(df_2025))

	# Plot active chapters (red) on the bottom
	bars_active = plt.bar(x_pos, df_2025["active_chapter_count"], bar_width, label="Active Chapters", color="#FF0000")

	# Plot inactive with history (light red) stacked on top
	bars_inactive_with_history = plt.bar(
		x_pos,
		df_2025["inactive_with_approved_history_count"],
		bar_width,
		bottom=df_2025["active_chapter_count"],
		label="Inactive (Approved History)",
		color="#FF8080"
	)

	# Plot inactive without rides (pink) stacked on top
	bars_inactive_without_rides = plt.bar(
		x_pos,
		df_2025["inactive_without_rides_count"],
		bar_width,
		bottom=df_2025["active_chapter_count"] + df_2025["inactive_with_approved_history_count"],
		label="Inactive (No Approved Rides)",
		color="#FFB3BA"
	)

	plt.title("Chapters by Country in 2025 (Active vs Inactive Breakdown)")
	plt.xlabel("Country")
	plt.ylabel("Chapter Count")
	plt.xticks(x_pos, df_2025["country"].tolist(), rotation=45, ha="right")
	plt.legend()

	for i in range(len(df_2025)):
		active = int(df_2025.iloc[i]["active_chapter_count"])
		inactive_with_history = int(df_2025.iloc[i]["inactive_with_approved_history_count"])
		inactive_without_rides = int(df_2025.iloc[i]["inactive_without_rides_count"])
		total = active + inactive_with_history + inactive_without_rides

		# Label for active chapters
		if active > 0:
			plt.text(i, active / 2, f"{active}", ha="center", va="center", fontsize=8, fontweight="bold", color="white")
		# Label for inactive with history
		if inactive_with_history > 0:
			plt.text(i, active + inactive_with_history / 2, f"{inactive_with_history}", ha="center", va="center", fontsize=8, fontweight="bold", color="white")
		# Label for inactive without rides
		if inactive_without_rides > 0:
			plt.text(i, active + inactive_with_history + inactive_without_rides / 2, f"{inactive_without_rides}", ha="center", va="center", fontsize=8, fontweight="bold", color="black")
		# Label for total chapters
		plt.text(i, total + 0.5, f"{total}", ha="center", va="bottom", fontsize=8, fontweight="bold")

	plt.tight_layout()
	plt.show()


def export_user_emails_csv(engine, output_path: str) -> None:
	df = query_to_dataframe(engine, USER_EMAILS_QUERY)
	if df.empty:
		print("No user emails found to export.")
		return

	df.to_csv(output_path, index=False)
	print(f"Exported {len(df)} unique emails to {output_path}")


def show_inactive_pilots(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, PILOTS_INACTIVE_QUERY)
	if df.empty:
		print("No inactive pilots found based on the current dataset window.")
		return

	export_dataframe(df, "inactive_pilots", export)
	print(df.to_string(index=False))


def plot_inactive_pilots(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, PILOTS_INACTIVE_QUERY)
	if df.empty:
		print("No inactive pilots found based on the current dataset window.")
		return

	export_dataframe(df, "inactive_pilots_plot", export)
	print(df.to_string(index=False))

	plot_df = df.copy()
	plot_df["bucket_start"] = plot_df["rides_completed"].apply(
		lambda rides_completed: rides_completed if rides_completed <= 10 else ((rides_completed - 11) // 25) * 25 + 11
	)
	plot_df["bucket_label"] = plot_df["bucket_start"].apply(
		lambda bucket_start: str(bucket_start) if bucket_start <= 10 else f"{bucket_start}-{bucket_start + 24}"
	)
	plot_df = plot_df.groupby(["bucket_start", "bucket_label"], as_index=False)["inactive_pilot_count"].sum()
	plot_rows = list(plot_df.itertuples(index=False, name=None))
	plot_rows.sort(key=lambda row: row[0])
	plot_df = pd.DataFrame(plot_rows, columns=["bucket_start", "bucket_label", "inactive_pilot_count"])

	plt.figure(figsize=(12, 6))
	bars = plt.bar(plot_df["bucket_label"], plot_df["inactive_pilot_count"])
	plt.title("Inactive Pilots by Completed Approved Rides")
	plt.xlabel("Approved Rides Completed")
	plt.ylabel("Inactive Pilot Count")
	plt.xticks(rotation=45, ha="right")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def show_pilot_trip_counts(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, PILOT_TRIP_COUNTS_QUERY)
	if df.empty:
		print("No pilot trip count data found.")
		return

	export_dataframe(df, "pilot_trip_counts", export)
	print(df.to_string(index=False))


def plot_pilot_trip_counts(engine, export: bool = False) -> None:
	df = query_to_dataframe(engine, PILOT_TRIP_COUNTS_QUERY)
	if df.empty:
		print("No pilot trip count data found.")
		return

	export_dataframe(df, "pilot_trip_counts_plot", export)
	print(df.to_string(index=False))

	plot_df = df.copy()
	plot_df["bucket_start"] = plot_df["rides_completed"].apply(
		lambda rides_completed: 0 if rides_completed == 0 else ((rides_completed - 1) // 20) * 20 + 1
	)
	plot_df["bucket_label"] = plot_df["bucket_start"].apply(
		lambda bucket_start: "0" if bucket_start == 0 else f"{bucket_start}-{bucket_start + 19}"
	)
	plot_df = (
		plot_df.groupby(["bucket_start", "bucket_label"], as_index=False)["pilot_count"]
		.sum()
	)
	plot_rows = list(plot_df.itertuples(index=False, name=None))
	plot_rows.sort(key=lambda row: row[0])
	plot_df = pd.DataFrame(plot_rows, columns=["bucket_start", "bucket_label", "pilot_count"])

	plt.figure(figsize=(12, 6))
	bars = plt.bar(plot_df["bucket_label"], plot_df["pilot_count"])
	plt.title("Pilot Counts by Completed Approved Ride Buckets")
	plt.xlabel("Approved Rides Completed")
	plt.ylabel("Pilot Count")
	plt.xticks(rotation=45, ha="right")
	for bar in bars:
		height = bar.get_height()
		plt.text(
			bar.get_x() + bar.get_width() / 2,
			height,
			f"{int(height)}",
			ha="center",
			va="bottom",
		)
	plt.tight_layout()
	plt.show()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Run query functions and plot results from local PostgreSQL."
	)
	parser.add_argument(
		"--run",
		nargs="+",
		choices=[
			"avg_duration",
			"us_chapter_count",
			"active_us_chapters",
			"active_chapters_in_denmark_by_year",
			"denmark_active_vs_joined_by_year",
			"world_chapters_by_year",
			"world_active_vs_joined_by_year",
			"total_chapters_by_country",
			"chapters_stacked_by_country",
			"user_emails_csv",
			"inactive_pilots",
			"inactive_pilots_plot",
			"pilot_trip_counts",
			"pilot_trip_counts_plot",
			"pilot_details_by_ride_count",
		],
		default=["inactive_pilots_plot"],
		help="Functions to run. Defaults to avg_duration.",
	)
	parser.add_argument(
		"--output",
		default="user_emails.csv",
		help="Output CSV path for user_emails_csv export.",
	)
	parser.add_argument(
		"--limit",
		type=int,
		default=100,
		help="Maximum rows to return for inactive_pilots.",
	)
	parser.add_argument(
		"--ride-count",
		type=int,
		default=1333,
		help="Exact approved ride count filter for pilot_details_by_ride_count.",
	)
	parser.add_argument(
		"--export",
		action="store_true",
		help="Export query results to CSV files in data_export/.",
	)
	return parser.parse_args()


def main() -> int:
	args = parse_args()
	selected = args.run or ["avg_duration"]
	engine = create_db_engine()

	function_map = {
		"avg_duration": lambda db_engine: plot_avg_trip_duration_by_country(db_engine, args.export),
		"us_chapter_count": lambda db_engine: plot_us_chapter_count(db_engine, args.export),
		"active_us_chapters": lambda db_engine: plot_active_us_chapters(db_engine, args.export),
		"active_chapters_in_denmark_by_year": lambda db_engine: plot_active_chapters_in_denmark_by_year(db_engine, args.export),
		"denmark_active_vs_joined_by_year": lambda db_engine: plot_denmark_active_vs_joined_by_year(db_engine, args.export),
		"world_chapters_by_year": lambda db_engine: plot_world_chapters_by_year(db_engine, args.export),
		"world_active_vs_joined_by_year": lambda db_engine: plot_world_active_vs_joined_by_year(db_engine, args.export),
		"total_chapters_by_country": lambda db_engine: plot_total_chapters_by_country(db_engine, args.export),
		"chapters_stacked_by_country": lambda db_engine: plot_chapters_stacked_by_country(db_engine, args.export),
		"inactive_pilots": lambda db_engine: show_inactive_pilots(db_engine, args.export),
		"inactive_pilots_plot": lambda db_engine: plot_inactive_pilots(db_engine, args.export),
		"pilot_trip_counts": lambda db_engine: show_pilot_trip_counts(db_engine, args.export),
		"pilot_trip_counts_plot": lambda db_engine: plot_pilot_trip_counts(db_engine, args.export),
		#"user_emails_csv": lambda db_engine: export_user_emails_csv(db_engine, args.output),
		"pilot_details_by_ride_count": lambda db_engine: show_pilot_details_by_ride_count(db_engine, args.ride_count, args.export),

	}

	try:
		for function_name in selected:
			function_map[function_name](engine)
		return 0
	except Exception as exc:
		print(f"Database query failed: {exc}")
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
