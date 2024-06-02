-- populate_fct_tables.sql
INSERT INTO dm.FactBookListRank (list_key, book_key, publisher_key, rank, published_date, time_key)
SELECT 
    (SELECT list_key FROM dm.DimList WHERE list_name = b.list_name),
    (SELECT book_key FROM dm.DimBook WHERE title = b.title AND author = b.author),
    (SELECT publisher_key FROM dm.DimPublisher WHERE publisher_name = b.publisher),
    b.rank,
    b.book_date,
    (SELECT time_key FROM dm.DimTime WHERE year = EXTRACT(YEAR FROM b.book_date) AND quarter = EXTRACT(QUARTER FROM b.book_date))
FROM ods.book b
JOIN ods.book_list bl ON b.book_date = bl.book_list_date AND b.list_name = bl.list_name
ON CONFLICT DO NOTHING;