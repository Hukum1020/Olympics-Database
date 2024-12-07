SELECT g.Season, a.Gender, COUNT(a.ID) AS athlete_count
FROM Athlete a
JOIN Sport_result sr ON a.ID = sr.Athlete_id
JOIN Games g ON sr.Event_name = g.Games
GROUP BY g.Season, a.Gender;