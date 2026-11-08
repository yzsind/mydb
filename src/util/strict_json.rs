//! Shared strict JSON visitor with caller-local serde specializations.

// Keep recursive serde specialization local to each caller.
macro_rules! define_strict_value {
    ($name:ident) => {
        struct $name(serde_json::Value);
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> serde::de::Visitor<'de> for V {
                    type Value = $name;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("bounded JSON")
                    }
                    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<$name, E> {
                        Ok($name(v.into()))
                    }
                    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<$name, E> {
                        Ok($name(v.into()))
                    }
                    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<$name, E> {
                        Ok($name(v.into()))
                    }
                    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<$name, E> {
                        serde_json::Number::from_f64(v)
                            .map(|n| $name(serde_json::Value::Number(n)))
                            .ok_or_else(|| E::custom("non-finite number"))
                    }
                    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<$name, E> {
                        Ok($name(v.into()))
                    }
                    fn visit_string<E: serde::de::Error>(self, v: String) -> Result<$name, E> {
                        Ok($name(v.into()))
                    }
                    fn visit_unit<E: serde::de::Error>(self) -> Result<$name, E> {
                        Ok($name(serde_json::Value::Null))
                    }
                    fn visit_seq<A: serde::de::SeqAccess<'de>>(
                        self,
                        mut a: A,
                    ) -> Result<$name, A::Error> {
                        let mut v = Vec::new();
                        while let Some(x) = a.next_element::<$name>()? {
                            v.push(x.0);
                        }
                        Ok($name(serde_json::Value::Array(v)))
                    }
                    fn visit_map<A: serde::de::MapAccess<'de>>(
                        self,
                        mut a: A,
                    ) -> Result<$name, A::Error> {
                        let mut m = serde_json::Map::new();
                        while let Some(k) = a.next_key::<String>()? {
                            if m.contains_key(&k) {
                                return Err(serde::de::Error::custom("duplicate JSON key"));
                            }
                            let v = a.next_value::<$name>()?;
                            m.insert(k, v.0);
                        }
                        Ok($name(serde_json::Value::Object(m)))
                    }
                }
                de.deserialize_any(V)
            }
        }
    };
}

pub(crate) use define_strict_value;
