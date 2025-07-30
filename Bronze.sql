create schema bronze;
use bronze;

-- (In This Bronze Schema The Raw Data Was Inserted Which is the Imported Dataset From the Kaggle)
-- (The Imported dataset is considered as a Table Which is Named as "uber_rides_raw")
-- (It Returns the data that is Imported From the Kaggle)
select * from bronze.uber_rides_raw;


