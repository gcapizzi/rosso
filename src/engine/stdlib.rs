use anyhow::Result;

use crate::redis;

pub struct MutexedHashMap {
    map: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

impl MutexedHashMap {
    pub fn new() -> Self {
        MutexedHashMap {
            map: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl redis::Engine for MutexedHashMap {
    fn call(&self, command: redis::Command) -> redis::Result {
        let mut map;
        if let Ok(m) = self.map.lock() {
            map = m;
        } else {
            return redis::Result::Error("Failed to lock Redis map".to_string());
        }

        match command {
            redis::Command::Get { key: redis::Key(k) } => map
                .get(&k)
                .map(|v| redis::Result::BulkString(v.clone()))
                .unwrap_or(redis::Result::Null),
            redis::Command::Set {
                key: redis::Key(k),
                value: redis::String(v),
                expiration: _,
                get: _,
                condition: _,
            } => {
                map.insert(k, v);
                redis::Result::Ok
            }
            redis::Command::Client => redis::Result::Ok,
            redis::Command::Incr { key: redis::Key(k) } => incr(&mut map, k)
                .map(|v| redis::Result::Integer(v))
                .unwrap_or_else(|e| redis::Result::Error(e.to_string())),
        }
    }
}

fn incr(map: &mut std::collections::HashMap<String, String>, key: String) -> Result<i64> {
    if let Some(value) = map.get(&key) {
        let mut new_value: i64 = value.parse()?;
        new_value += 1;
        map.insert(key, new_value.to_string());
        Ok(new_value)
    } else {
        map.insert(key, "1".to_string());
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::Engine;
    use crate::redis::Key;
    use crate::redis::String;

    #[test]
    fn test_set_and_get() {
        let redis = MutexedHashMap::new();

        let result = redis.call(redis::Command::Set {
            key: Key("key".to_string()),
            value: String("value".to_string()),
            expiration: None,
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Get {
            key: Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let redis = MutexedHashMap::new();

        let result = redis.call(redis::Command::Get {
            key: Key("nonexistent".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_client() {
        let redis = MutexedHashMap::new();

        let result = redis.call(redis::Command::Client);
        assert_eq!(result, redis::Result::Ok);
    }

    #[test]
    fn test_incr() {
        let redis = MutexedHashMap::new();

        let result = redis.call(redis::Command::Incr {
            key: Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(1));

        let result = redis.call(redis::Command::Incr {
            key: Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(2));
    }
}
