use gold;

-- (Creating a Table with Monthly Rides Summary)
CREATE TABLE gold.monthly_rides_summary (
    year INT,
    month INT,
    ride_category VARCHAR(50),
    ride_purpose VARCHAR(100),
    total_rides INT,
    total_miles FLOAT,
    avg_miles_per_ride FLOAT,
    total_duration_minutes INT,
    avg_duration_minutes FLOAT,
    avg_speed_mph FLOAT
);

-- (Insering the data into the table)
INSERT INTO gold.monthly_rides_summary
SELECT 
    year,
    month,
    ride_category,
    ride_purpose,
    COUNT(*) AS total_rides,
    SUM(miles) AS total_miles,
    AVG(miles) AS avg_miles_per_ride,
    SUM(duration_minutes) AS total_duration_minutes,
    AVG(duration_minutes) AS avg_duration_minutes,
    AVG(avg_speed_mph) AS avg_speed_mph
FROM silver.uber_rides_cleansed
GROUP BY year, month, ride_category, ride_purpose;

-- (Returns the all categories and all information regarding the dataset according to the month by comapring the purpose)
select * from gold.monthly_rides_summary;



-- (Creating the table on Daily rides on the dataset)
CREATE TABLE gold.daily_rides_summary (
    ride_date DATE,
    ride_category VARCHAR(50),
    total_rides INT,
    total_miles FLOAT,
    total_minutes INT
);
-- (Insertinng the data into the Table)
INSERT INTO gold.daily_rides_summary
SELECT 
    ride_date,
    ride_category,
    COUNT(*) AS total_rides,
    SUM(miles) AS total_miles,
    SUM(duration_minutes) AS total_minutes
FROM silver.uber_rides_cleansed
GROUP BY ride_date, ride_category;

-- (Returns the Number of Rides perday and number of miles and nuber of minitues perday)
select * from gold.daily_rides_summary;

-- (Creating a Table on Top Locations on the Dataset)
CREATE TABLE gold.top_locations (
    start_location VARCHAR(255),
    ride_count INT,
    total_miles FLOAT,
    avg_miles FLOAT,
    total_minutes INT,
    avg_minutes FLOAT
);

-- (Inserting the data into the Table)
INSERT INTO gold.top_locations
SELECT 
    start_location,
    COUNT(*) AS ride_count,
    SUM(miles) AS total_miles,
    AVG(miles) AS avg_miles,
    SUM(duration_minutes) AS total_minutes,
    AVG(duration_minutes) AS avg_minutes
FROM silver.uber_rides_cleansed
GROUP BY start_location
HAVING COUNT(*) > 5
ORDER BY ride_count DESC;

-- (Returns the Top Locations of the dataset by the whole year by caluculating the number of rides booked to the location it gives the data orderwise)
select * from gold.top_locations;


-- (Creating the Table on Time of Day Analysis)
CREATE TABLE gold.time_of_day_analysis (
    time_of_day VARCHAR(50),
    ride_category VARCHAR(50),
    total_rides INT,
    total_miles FLOAT,
    avg_miles FLOAT,
    avg_duration_minutes FLOAT
);

-- (Inseritng The data into the Table)
INSERT INTO gold.time_of_day_analysis
SELECT 
    CASE 
        WHEN hour BETWEEN 5 AND 11 THEN 'Morning (5-11)'
        WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon (12-16)'
        WHEN hour BETWEEN 17 AND 21 THEN 'Evening (17-21)'
        ELSE 'Night (22-4)'
    END AS time_of_day,
    ride_category,
    COUNT(*) AS total_rides,
    SUM(miles) AS total_miles,
    AVG(miles) AS avg_miles,
    AVG(duration_minutes) AS avg_duration_minutes
FROM silver.uber_rides_cleansed
GROUP BY time_of_day, ride_category;

-- (It Returns the time of the day where the rides booked more like evening or night or morning or noon etcc... based on the number of rides all over the year)
select * from gold.time_of_day_analysis;

