SELECT t.Country_Code, COUNT(sr.Medal) AS medal_count
FROM Sport_result sr
JOIN Athlete a ON sr.Athlete_id = a.ID
JOIN Team t ON a.Team = t.Team
JOIN Games g ON sr.Event_name = g.Games
WHERE g.Year_ = 2016 AND sr.Medal != 'NA'
GROUP BY t.Country_Code
ORDER BY medal_count DESC;