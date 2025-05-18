use std::collections::HashMap;

use crate::redis;

pub struct Redis {
    map: HashMap<String, String>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            map: HashMap::new(),
        }
    }

    pub fn call(&mut self, command: redis::Command) -> redis::Result {
        match command {
            redis::Command::Get { key: redis::Key(k) } => self
                .map
                .get(&k)
                .map(|v| redis::Result::BulkString(v.clone()))
                .unwrap_or(redis::Result::Null),
            redis::Command::Set {
                key: redis::Key(k),
                value: redis::String(v),
            } => {
                self.map.insert(k, v);
                redis::Result::Ok
            }
            redis::Command::Client => redis::Result::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::Key;
    use crate::redis::String;

    #[test]
    fn test_set_and_get() {
        let mut redis = Redis::new();

        let result = redis.call(redis::Command::Set {
            key: Key("key".to_string()),
            value: String("value".to_string()),
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Get {
            key: Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let mut redis = Redis::new();

        let result = redis.call(redis::Command::Get {
            key: Key("nonexistent".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_client() {
        let mut redis = Redis::new();

        let result = redis.call(redis::Command::Client);
        assert_eq!(result, redis::Result::Ok);
    }
}
