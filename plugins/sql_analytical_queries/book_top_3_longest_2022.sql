SELECT b.title, b.author
FROM dm.FactBookListRank f
INNER JOIN dm.DimBook b ON f.book_key = b.book_key
WHERE f.rank <= 3 AND f.published_date >= '2022-01-01' AND f.published_date < '2023-01-01'
GROUP BY b.book_key, b.title, b.author
ORDER BY SUM(CASE WHEN f.rank <= 3 THEN 1 ELSE 0 END) DESC
LIMIT 1;