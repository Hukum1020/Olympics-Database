SELECT sr.Sport_name, AVG(a.Age) AS average_age
FROM Athlete a
JOIN Sport_result sr ON a.ID = sr.Athlete_id
GROUP BY sr.Sport_name
ORDER BY average_age DESC;