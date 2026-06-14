use serde::Deserialize;

// ---------------------------------------------------------------------------
// PandaScore Low Latency frame — single struct, branch on field presence
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub(crate) struct PandaScoreLLFrame {
    #[serde(rename = "type")]
    pub frame_type: Option<String>,
    pub payload: Option<PandaScoreHelloPayload>,

    pub match_id: Option<i64>,
    pub game_id: Option<i64>,
    pub game_status: Option<String>,
    pub match_status: Option<String>,
    pub updated_at: Option<String>,

    pub home_team: Option<PandaScoreTeam>,
    pub away_team: Option<PandaScoreTeam>,
    pub current_round: Option<PandaScoreRound>,
    pub map_id: Option<i64>,
}

#[derive(Deserialize)]
pub(crate) struct PandaScoreHelloPayload {
    pub match_id: String,
    pub match_status: String,
    pub videogame: String,
}

#[derive(Deserialize)]
pub(crate) struct PandaScoreTeam {
    pub team_id: i64,
    pub team_name: String,
    pub score: i64,
}

#[derive(Deserialize)]
pub(crate) struct PandaScoreRound {
    pub round_number: i64,
    pub timer: Option<i64>,
}

impl PandaScoreLLFrame {
    pub(crate) fn is_hello(&self) -> bool {
        self.frame_type.as_deref() == Some("hello")
    }

    pub(crate) fn is_game_state(&self) -> bool {
        self.home_team.is_some()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hello_frame() {
        let json = r#"{
            "type": "hello",
            "payload": {
                "match_id": "1513141",
                "match_status": "not_started",
                "videogame": "csgo"
            }
        }"#;
        let frame: PandaScoreLLFrame = serde_json::from_str(json).unwrap();
        assert!(frame.is_hello());
        assert!(!frame.is_game_state());
        let payload = frame.payload.unwrap();
        assert_eq!(payload.match_id, "1513141");
        assert_eq!(payload.match_status, "not_started");
        assert_eq!(payload.videogame, "csgo");
        assert!(frame.home_team.is_none());
    }

    #[test]
    fn parse_game_state_frame() {
        let json = r#"{
            "updated_at": "2026-06-12T15:16:58.675308Z",
            "game_id": 219829,
            "match_id": 1513132,
            "match_status": "running",
            "game_status": "running",
            "home_team": {"team_id": 3216, "team_name": "Natus Vincere", "score": 12},
            "away_team": {"team_id": 133708, "team_name": "Legacy", "score": 4},
            "current_round": {
                "round_number": 17,
                "timer": 82,
                "bomb": {"status": "planted"},
                "home_team": {
                    "team_id": 3216,
                    "side": "terrorists",
                    "players": [{"alive": true, "name": "Aleksib"}]
                },
                "away_team": {
                    "team_id": 133708,
                    "side": "counter_terrorists",
                    "players": [{"alive": true, "name": "arT"}]
                }
            },
            "map_id": 2
        }"#;
        let frame: PandaScoreLLFrame = serde_json::from_str(json).unwrap();
        assert!(!frame.is_hello());
        assert!(frame.is_game_state());
        assert_eq!(frame.match_id, Some(1513132));
        assert_eq!(frame.game_id, Some(219829));
        assert_eq!(frame.game_status.as_deref(), Some("running"));

        let home = frame.home_team.unwrap();
        assert_eq!(home.team_id, 3216);
        assert_eq!(home.team_name, "Natus Vincere");
        assert_eq!(home.score, 12);

        let away = frame.away_team.unwrap();
        assert_eq!(away.team_id, 133708);
        assert_eq!(away.score, 4);

        let round = frame.current_round.unwrap();
        assert_eq!(round.round_number, 17);
        assert_eq!(round.timer, Some(82));
    }

    #[test]
    fn parse_status_change_frame() {
        let json = r#"{
            "updated_at": "2026-06-12T15:21:36.628Z",
            "match_id": 1513132,
            "game_id": 219829,
            "game_status": "finished",
            "match_status": "running"
        }"#;
        let frame: PandaScoreLLFrame = serde_json::from_str(json).unwrap();
        assert!(!frame.is_hello());
        assert!(!frame.is_game_state());
        assert_eq!(frame.game_status.as_deref(), Some("finished"));
        assert_eq!(frame.match_status.as_deref(), Some("running"));
        assert!(frame.home_team.is_none());
        assert!(frame.current_round.is_none());
    }

    #[test]
    fn parse_map_start_frame() {
        let json = r#"{
            "updated_at": "2026-06-12T15:21:15.240460Z",
            "match_id": 1513132,
            "game_id": 219830,
            "game_status": "not_started",
            "match_status": "running",
            "map_id": 5,
            "home_team": {"team_id": 3216, "team_name": "Natus Vincere", "score": 0},
            "away_team": {"team_id": 133708, "team_name": "Legacy", "score": 0},
            "current_round": {"round_number": 1, "timer": 115}
        }"#;
        let frame: PandaScoreLLFrame = serde_json::from_str(json).unwrap();
        assert!(frame.is_game_state());
        assert_eq!(frame.game_status.as_deref(), Some("not_started"));
        assert_eq!(frame.game_id, Some(219830));

        let home = frame.home_team.unwrap();
        assert_eq!(home.score, 0);
    }

    #[test]
    fn unknown_fields_ignored() {
        let json = r#"{
            "updated_at": "2026-06-12T15:16:58Z",
            "match_id": 1513132,
            "game_id": 219829,
            "game_status": "running",
            "match_status": "running",
            "home_team": {"team_id": 3216, "team_name": "Navi", "score": 5},
            "away_team": {"team_id": 133708, "team_name": "Legacy", "score": 3},
            "current_round": {
                "round_number": 9,
                "timer": 50,
                "bomb": {"status": null},
                "home_team": {"team_id": 3216, "side": "terrorists", "players": []},
                "away_team": {"team_id": 133708, "side": "counter_terrorists", "players": []}
            },
            "map_id": 2,
            "some_future_field": "ignored"
        }"#;
        let frame: PandaScoreLLFrame = serde_json::from_str(json).unwrap();
        assert_eq!(frame.home_team.unwrap().score, 5);
    }
}
