CREATE TABLE IF NOT EXISTS competitions(
    competition_id INTEGER PRIMARY KEY,
    competition_name TEXT
);

CREATE TABLE IF NOT EXISTS teams(
    team_id INTEGER PRIMARY KEY,
    name TEXT,
    abbreviation VARCHAR(5),
    stadium TEXT,
    address TEXT,
    competition_id INTEGER REFERENCES competitions(competition_id)
);

CREATE TABLE IF NOT EXISTS players(
    player_id INTEGER PRIMARY KEY,
    name TEXT,
    team_id INTEGER REFERENCES teams(team_id),
    position TEXT,
    date_of_birth DATE,
    nationality TEXT
);

