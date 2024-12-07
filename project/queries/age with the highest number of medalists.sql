SELECT a.Age, COUNT(sr.Athlete_id) AS medalist_count
FROM Athlete a
JOIN Sport_result sr ON a.ID = sr.Athlete_id
WHERE sr.Medal != 'NA'
GROUP BY a.Age
ORDER BY medalist_count DESC
LIMIT 1;
