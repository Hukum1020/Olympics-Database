SELECT t.Country_Code, sr.Sport_name, AVG(a.Height) AS avg_height
FROM Athlete a
JOIN Sport_result sr ON a.ID = sr.Athlete_id
JOIN Team t ON a.Team = t.Team
GROUP BY t.Country_Code, sr.Sport_name
ORDER BY avg_height DESC
LIMIT 10;