# New York Citi Bike trips

`triplets.csv.gz` holds one row per bike rental trip: `start_station_id`, `end_station_id`.

It is a trimmed copy of the [New York Bike trips](https://www.kaggle.com/datasets/gabrielramos87/bike-trips)
Kaggle dataset (NYC Citi Bike trip data): 1,595,334 trips over 774 stations, 5.3 MB gzipped. Only the two
station id columns are kept, as that is all the examples using it need.

Kaggle returns 404 for dataset downloads from cloud IPs, so the examples read this copy from the
repository instead of downloading the original at runtime. To regenerate it:

```bash
curl -sSL "https://www.kaggle.com/api/v1/datasets/download/gabrielramos87/bike-trips" -o archive.zip
unzip -p archive.zip "New York Citibike Trips.csv" | python3 -c "
import csv, gzip, sys

writer = csv.writer(gzip.open('triplets.csv.gz', 'wt', newline=''))
writer.writerow(['start_station_id', 'end_station_id'])
for row in csv.DictReader(sys.stdin):
    writer.writerow([row['start_station_id'], row['end_station_id']])
"
```
