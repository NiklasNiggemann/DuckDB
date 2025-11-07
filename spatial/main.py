import duckdb
import os

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')

con = duckdb.connect('md:_share/foursquare/0cbf467d-03b0-449e-863a-ce17975d2c0b')
con.install_extension("spatial")
con.load_extension("spatial")

# clusters of shops within a 2km radius of Biel, Switzerland, where neighboring businesses are located within 2 meters
# of each other, identifying extremely close commercial pairings that likely share walls or entrances
con.sql("""
WITH base_location AS (
  SELECT 
    ST_Point(7.2474174805428335, 47.13673837848461) as center  -- Biel, Switzerland
),
nearby_stores AS (
SELECT 
    fsq_place_id,
    name, 
    longitude, 
    latitude,
    ST_Point(longitude, latitude) as location,
    -- Calculate distance in meters
    ROUND(ST_Distance_Spheroid(
        ST_Point(longitude, latitude), 
        base_location.center
    )::numeric, 2) as distance_meters
FROM fsq_os_places, base_location
WHERE date_closed IS NULL
    -- Use bounding box for initial filtering
    AND longitude BETWEEN 7.0 AND 7.5
    AND latitude BETWEEN 46.9 AND 47.3
    -- Then apply precise distance filter
    AND ST_Distance_Spheroid(
        ST_Point(longitude, latitude), 
        base_location.center
    ) <= 2000  -- 2km radius
)
 SELECT 
  a.name as store1, CAST(a.latitude AS VARCHAR) || ', ' || CAST(a.longitude AS VARCHAR) as location,
  b.name as store2, CAST(b.latitude AS VARCHAR) || ', ' || CAST(b.longitude AS VARCHAR) as location,
  ROUND(ST_Distance(a.location, b.location), 2) as distance_meters
FROM nearby_stores a
JOIN nearby_stores b 
  ON a.fsq_place_id < b.fsq_place_id
  AND ST_DWithin(a.location, b.location, 2)  -- Looking for stores within Xm of each other
ORDER BY distance_meters
LIMIT 20000; 
""").show()