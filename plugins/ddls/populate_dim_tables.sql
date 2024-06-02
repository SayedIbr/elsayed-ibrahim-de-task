-- populate_dim_tables.sql

INSERT INTO dm.DimTime (year, quarter)
SELECT DISTINCT EXTRACT(YEAR FROM book_list_date) AS year, EXTRACT(QUARTER FROM book_list_date) AS quarter
FROM ods.book_list
ON CONFLICT DO NOTHING;

INSERT INTO dm.DimList (list_name, display_name)
SELECT DISTINCT list_name, display_name
FROM ods.book_list
ON CONFLICT DO NOTHING;

INSERT INTO dm.DimBook (title, author)
SELECT DISTINCT title, author
FROM ods.book
ON CONFLICT DO NOTHING;

INSERT INTO dm.DimPublisher (publisher_name)
SELECT DISTINCT publisher
FROM ods.book
ON CONFLICT DO NOTHING;


