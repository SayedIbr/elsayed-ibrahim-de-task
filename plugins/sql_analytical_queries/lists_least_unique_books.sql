WITH RankedBooks AS (
  SELECT l.list_name, f.book_key
  FROM dm.FactBookListRank f
  INNER JOIN dm.DimList l ON f.list_key = l.list_key
),
UniqueBooksPerList AS (
  SELECT list_name, COUNT(DISTINCT book_key) AS unique_books
  FROM RankedBooks
  GROUP BY list_name
)
SELECT list_name, unique_books
FROM UniqueBooksPerList
ORDER BY unique_books ASC
LIMIT 3;
