SELECT CASE WHEN rank = 1 THEN 'Jake' ELSE 'Pete' END AS team,
       b.title
FROM dm.FactBookListRank f
INNER JOIN dm.DimBook b ON f.book_key = b.book_key
INNER JOIN dm.DimList l ON f.list_key = l.list_key
WHERE f.rank IN (1, 3) AND f.published_date >= '2023-01-01' AND f.published_date < '2024-01-01';
