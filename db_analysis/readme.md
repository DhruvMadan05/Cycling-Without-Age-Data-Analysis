# Flags
## export
will export the data to a csv file int the data_export folder
```--export```


# number of rides per pilot
python data.py --run pilot_trip_counts_plot

# inactive pilots
python data.py --run inactive_pilots_plot

# pilot info for specifc ride count
python data.py --run pilot_details_by_ride_count --ride-count 1333

# Chapters by Country

## Active chapters per country
python data.py --run active_us_chapters

## Total Chapter by country
python data.py --run total_chapters_by_country

## Active chapters in denmark by year
python data.py --run active_chapters_in_denmark_by_year --export

## Active vs inactive chapter in denmark
python data.py --run denmark_active_vs_joined_by_year --export

## Active vs inactive chapter worldwide by year
python data.py --run world_chapters_by_year --export

## Active vs inactive chapers by country by year
python data.py --run world_active_vs_joined_by_year --export

## Stacked Box Plot for active and inactive chapters
python data.py --run chapters_stacked_by_country

# Number of rides per number of pilots per chapter by country

# How mnay cancelled rides are there

# Rides per month per country

# emails of active chapters by country

# emails of active pilots by country & role

# email list of inactive pilots 
