CREATE TABLE competitions
(
    competition_id UInt32,
    competition_name String
)
ENGINE = MergeTree
ORDER BY (competition_id);

CREATE TABLE teams
(
    team_id UInt32,
    name String,
    abbreviation Nullable(String),
    stadium Nullable(String),
    address Nullable(String),
    competition_id UInt32
)
ENGINE = MergeTree
ORDER BY (competition_id, team_id);

CREATE TABLE players
(
    player_id UInt32,
    team_id UInt32,
    name String,
    position Nullable(String),
    date_of_birth Nullable(Date),
    nationality Nullable(String)
)
ENGINE = MergeTree
ORDER BY (team_id, player_id);

CREATE TABLE matches
(
    match_id UInt32,
    competition_id UInt32,
    home_team_id UInt32,
    home_team String,
    away_team_id UInt32,
    away_team String,
    scores_home_team UInt8,
    scores_away_team UInt8,
    referee_id Nullable(UInt32),
    referee_name Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (match_id);