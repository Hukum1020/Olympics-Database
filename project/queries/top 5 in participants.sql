SELECT sr.Sport_name, COUNT(sr.Athlete_id) AS participant_count
FROM Sport_result sr
GROUP BY sr.Sport_name
ORDER BY participant_count DESC
LIMIT 5;