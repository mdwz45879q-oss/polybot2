use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::Deserialize;
use std::fmt;

#[derive(Deserialize)]
pub(crate) struct KalstropFrame<'a> {
    #[serde(rename = "type", default)]
    pub msg_type: &'a str,
    #[serde(borrow)]
    pub payload: Option<KalstropPayload<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct KalstropPayload<'a> {
    #[serde(borrow)]
    pub data: Option<KalstropData<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct KalstropData<'a> {
    #[serde(borrow, rename = "sportsMatchStateUpdatedV2")]
    pub update: Option<KalstropUpdate<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct KalstropUpdate<'a> {
    #[serde(rename = "fixtureId", default)]
    pub fixture_id: &'a str,
    #[serde(borrow, rename = "matchSummary")]
    pub match_summary: Option<KalstropMatchSummary<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct KalstropMatchSummary<'a> {
    #[serde(rename = "homeScore")]
    pub home_score: Option<&'a str>,
    #[serde(rename = "awayScore")]
    pub away_score: Option<&'a str>,
    #[serde(
        rename = "matchStatusDisplay",
        default,
        deserialize_with = "deserialize_first_free_text"
    )]
    pub first_free_text: Option<&'a str>,
    #[serde(default)]
    #[allow(dead_code)]
    pub statistics: serde::de::IgnoredAny,
}

/// Custom deserializer that reads a JSON array of objects and extracts
/// the `freeText` field from the first element only, without allocating a Vec.
fn deserialize_first_free_text<'de, D>(deserializer: D) -> Result<Option<&'de str>, D::Error>
where
    D: Deserializer<'de>,
{
    struct FirstFreeTextVisitor;

    #[derive(Deserialize)]
    struct Entry<'a> {
        #[serde(rename = "freeText")]
        free_text: Option<&'a str>,
    }

    impl<'de> Visitor<'de> for FirstFreeTextVisitor {
        type Value = Option<&'de str>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array with optional freeText entries")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if let Some(entry) = seq.next_element::<Entry<'de>>()? {
                // Skip remaining elements
                while seq.next_element::<de::IgnoredAny>()?.is_some() {}
                return Ok(entry.free_text);
            }
            Ok(None)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }
    }

    deserializer.deserialize_any(FirstFreeTextVisitor)
}
