# Book2go Database Setup Guide

This guide explains how to set up a local PostgreSQL database and import the Book2go dataset.

---

## Prerequisites

- **PostgreSQL 15+** installed on your system
- **psql** command-line tool (included with PostgreSQL)

### Installing PostgreSQL

**macOS (Homebrew):**
```bash
brew install postgresql@16
brew services start postgresql@16
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-client
sudo systemctl start postgresql
```

**Windows:**
Download and install from https://www.postgresql.org/download/windows/

---

## Step 1: Create a Database

Open a terminal and run:

```bash
# Connect to PostgreSQL as the default user
psql -U postgres

# Inside psql, create a new database:
CREATE DATABASE book2go;

# Exit psql
\q
```

---

## Step 2: Import the Schema

The schema file (`schema.sql`) contains all table definitions, indexes, and constraints.

```bash
psql -U postgres -d book2go -f schema.sql
```

You may see some warnings about extensions (pg_cron, pgsodium, etc.) that are Supabase-specific and not available locally. These can be safely ignored — they do not affect the data tables.

---

## Step 3: Import the Data

The data file (`data.sql`) contains all records using PostgreSQL COPY statements.

```bash
psql -U postgres -d book2go -f data.sql
```

---

## Step 4: Verify the Import

Connect to the database and check the tables:

```bash
psql -U postgres -d book2go
```

```sql
-- List all tables
\dt public.*

-- Check row counts
SELECT 'user' AS table_name, COUNT(*) FROM public."user"
UNION ALL SELECT 'community', COUNT(*) FROM public.community
UNION ALL SELECT 'trip', COUNT(*) FROM public.trip
UNION ALL SELECT 'trip_participants', COUNT(*) FROM public.trip_participants
UNION ALL SELECT 'resource', COUNT(*) FROM public.resource
UNION ALL SELECT 'community_users', COUNT(*) FROM public.community_users
UNION ALL SELECT 'roles', COUNT(*) FROM public.roles;
```

---

## Common Issues

### Extension errors during schema import
Warnings like `ERROR: extension "pg_cron" is not available` are expected. These are Supabase-specific extensions. The core tables will still be created correctly.

### Permission errors
If you get permission errors, make sure you are connecting as a user with CREATE privileges on the database. On a fresh local install, the `postgres` superuser should work.

### "role does not exist" warnings
The schema references Supabase-specific roles (e.g., `authenticated`, `service_role`). These warnings can be ignored for local data analysis.

### Table name requires quotes
The `user` table has a reserved name in PostgreSQL. Always quote it:
```sql
SELECT * FROM public."user" LIMIT 10;
```

---

## Example Queries

### Total number of trips per country
```sql
SELECT c.country, COUNT(t.id) AS trip_count
FROM public.trip t
JOIN public.community c ON t.community_id = c.id
WHERE t.status = 'approved'
GROUP BY c.country
ORDER BY trip_count DESC;
```

### Monthly trip trends
```sql
SELECT
  DATE_TRUNC('month', t.start) AS month,
  COUNT(*) AS trips
FROM public.trip t
WHERE t.status = 'approved'
GROUP BY month
ORDER BY month;
```

### Number of active pilots per community
```sql
SELECT c.name AS community, COUNT(DISTINCT cu.user_id) AS pilot_count
FROM public.community_users cu
JOIN public.roles r ON cu.role_id = r.id
JOIN public.community c ON cu.community_id = c.id
WHERE r.name = 'pilot'
GROUP BY c.name
ORDER BY pilot_count DESC;
```

### Average trip duration (in minutes)
```sql
SELECT
  c.country,
  ROUND(AVG(EXTRACT(EPOCH FROM (t."end" - t.start)) / 60), 1) AS avg_minutes
FROM public.trip t
JOIN public.community c ON t.community_id = c.id
WHERE t.status = 'approved' AND t."end" IS NOT NULL
GROUP BY c.country
ORDER BY avg_minutes DESC;
```

### Passengers per trip
```sql
SELECT
  t.id AS trip_id,
  t.start,
  c.name AS community,
  COUNT(tp.id) AS passenger_count
FROM public.trip t
JOIN public.community c ON t.community_id = c.id
LEFT JOIN public.trip_participants tp ON tp.trip_id = t.id
WHERE t.status = 'approved'
GROUP BY t.id, t.start, c.name
ORDER BY t.start DESC
LIMIT 20;
```

---

## Data Privacy

This dataset contains personally identifiable information (PII). See `DATABASE_DOCUMENTATION.md` for privacy requirements.
