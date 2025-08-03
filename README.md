# 🚖 Uber Rides Analytics Pipeline | SQL Warehouse Project

Built an end-to-end SQL data pipeline that processed over 10k+ Uber ride records using a layered warehouse architecture: Bronze (raw), Silver (cleansed), and Gold (aggregated).

Data was sourced from structured .csv files and ingested into the Bronze layer. In the Silver layer, applied SQL transformations including DATE functions, STRING operations , and CASE statements to clean and standardize the data. Optimized performance by 40% through indexing and efficient query design.

The final transformed data was stored as structured tables in a relational database within a SQL warehouse, forming the Gold layer. These tables powered aggregated views that revealed key business insights like peak ride times and high-demand locations.
