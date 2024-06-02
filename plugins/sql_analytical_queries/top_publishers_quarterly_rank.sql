WITH PublisherPoints AS (
  SELECT f.published_date, d.year, d.quarter,
         b.book_key, f.publisher_key,  -- Use f.publisher_key from FactBookListRank
         CASE WHEN f.rank = 1 THEN 5
              WHEN f.rank = 2 THEN 4
              WHEN f.rank = 3 THEN 3
              WHEN f.rank = 4 THEN 2
              ELSE 1
         END AS points
  FROM dm.FactBookListRank f
  INNER JOIN dm.DimBook b ON f.book_key = b.book_key
  INNER JOIN dm.DimPublisher p ON f.publisher_key = p.publisher_key  -- Use f.publisher_key here
  INNER JOIN dm.DimTime d ON f.time_key = d.time_key
  WHERE f.published_date >= '2021-01-01' AND f.published_date < '2024-01-01'
)
SELECT year, quarter, publisher_name, SUM(points) AS total_points
FROM PublisherPoints
INNER JOIN dm.DimPublisher pub ON PublisherPoints.publisher_key = pub.publisher_key
GROUP BY year, quarter, publisher_name
ORDER BY year, quarter, total_points DESC
LIMIT 5;