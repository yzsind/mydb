//! Catalog name identity stays segmented until binding supplies a database.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "NameParts")]
pub struct QualifiedName {
    database: Option<String>,
    object: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NameParts {
    database: Option<String>,
    object: String,
}

impl TryFrom<NameParts> for QualifiedName {
    type Error = &'static str;
    fn try_from(parts: NameParts) -> Result<Self, Self::Error> {
        Self::new(parts.database, parts.object)
    }
}

impl QualifiedName {
    pub fn new(database: Option<String>, object: String) -> Result<Self, &'static str> {
        // The current catalog uses '.' as its database/object separator.
        // Reject unrepresentable components instead of binding another object.
        for part in database.iter().chain(std::iter::once(&object)) {
            if part.is_empty() || part.contains('.') || part.contains('\0') {
                return Err(
                    "catalog name components must be nonempty and contain neither '.' nor NUL",
                );
            }
        }
        Ok(Self { database, object })
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }
    pub fn object(&self) -> &str {
        &self.object
    }
    pub fn bind<'a>(&'a self, default_database: &'a str) -> BoundName<'a> {
        BoundName {
            database: self.database.as_deref().unwrap_or(default_database),
            object: &self.object,
        }
    }
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{database}.")?;
        }
        f.write_str(&self.object)
    }
}

/// A bound catalog identity, independent of SQL quoting and display syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundName<'a> {
    pub database: &'a str,
    pub object: &'a str,
}

impl BoundName<'_> {
    pub fn catalog_key(self) -> String {
        let mut key = String::with_capacity(self.database.len() + self.object.len() + 1);
        key.push_str(self.database);
        key.push('.');
        key.push_str(self.object);
        key
    }
}
