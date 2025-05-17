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
