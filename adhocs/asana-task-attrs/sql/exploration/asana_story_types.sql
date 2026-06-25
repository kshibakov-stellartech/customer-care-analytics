SELECT type, COUNT(*) AS cnt
FROM fivetran_asana.story
GROUP BY 1
ORDER BY 2 DESC;
