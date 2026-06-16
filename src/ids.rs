use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}-{}", $prefix, Uuid::new_v4()))
            }

            pub fn from_string(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

id_type!(NodeId, "node");
id_type!(GenomeId, "genome");
id_type!(TaskId, "task");
id_type!(DirectiveId, "directive");
id_type!(MessageId, "msg");
id_type!(RelationId, "rel");
id_type!(ChannelId, "chan");
id_type!(MutationId, "mut");
id_type!(TraceId, "trace");
id_type!(SuggestionId, "suggestion");
