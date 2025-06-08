use anyhow::Result;

use crate::redis;

#[derive(Debug)]
struct Expirable<T> {
    pub value: T,
    expires_at: Option<std::time::Instant>,
}

impl<T> Expirable<T> {
    pub fn new(value: T, expires_at: Option<std::time::Instant>) -> Self {
        Expirable { value, expires_at }
    }

    pub fn new_perpetual(value: T) -> Self {
        Self::new(value, None)
    }

    fn is_expired(&self, now: std::time::Instant) -> bool {
        self.expires_at.map_or(false, |t| t <= now)
    }
}

pub trait Clock {
    fn now(&self) -> std::time::Instant;
}

pub struct StdClock;

impl Clock for StdClock {
    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }
}

pub struct ConcurrentHashMap<'a, C = StdClock> {
    map: scc::HashMap<String, Expirable<String>>,
    clock: &'a C,
}

impl ConcurrentHashMap<'_> {
    pub fn new() -> Self {
        ConcurrentHashMap {
            map: scc::HashMap::new(),
            clock: &StdClock,
        }
    }

    pub fn with_clock<C: Clock>(clock: &C) -> ConcurrentHashMap<C> {
        ConcurrentHashMap {
            map: scc::HashMap::new(),
            clock,
        }
    }
}

impl<C: Clock> redis::Engine for ConcurrentHashMap<'_, C> {
    fn call(&self, command: redis::Command) -> redis::Result {
        match command {
            redis::Command::Get { key: redis::Key(k) } => self
                .get(k)
                .map(|v| redis::Result::BulkString(v))
                .unwrap_or(redis::Result::Null),
            redis::Command::Set {
                key: redis::Key(k),
                value: redis::String(v),
                expiration,
            } => {
                let ex = expiration.map(|e| match e {
                    redis::Expiration::Seconds(redis::Integer(secs)) => {
                        self.clock.now() + std::time::Duration::from_secs(secs as u64)
                    }
                    redis::Expiration::Milliseconds(redis::Integer(millis)) => {
                        self.clock.now() + std::time::Duration::from_millis(millis as u64)
                    }
                });
                self.set(k, v, ex)
                    .map(|_| redis::Result::Ok)
                    .unwrap_or_else(|e| redis::Result::Error(e.to_string()))
            }
            redis::Command::Client => redis::Result::Ok,
            redis::Command::Incr { key: redis::Key(k) } => self
                .incr(k)
                .map(|v| redis::Result::Integer(v))
                .unwrap_or_else(|e| redis::Result::Error(e.to_string())),
        }
    }
}

impl<C: Clock> ConcurrentHashMap<'_, C> {
    fn get(&self, key: String) -> Option<String> {
        self.map.remove_if(&key, |e| e.is_expired(self.clock.now()));
        self.map.read(&key, |_, e| e.value.clone())
    }

    fn set(
        &self,
        key: String,
        value: String,
        expires_at: Option<std::time::Instant>,
    ) -> Result<()> {
        self.map.upsert(key, Expirable::new(value, expires_at));
        Ok(())
    }

    fn incr(&self, key: String) -> Result<i64> {
        if let Some(mut expirable) = self.map.get(&key) {
            let mut new_value: i64 = expirable.value.parse()?;
            new_value += 1;
            expirable.value = new_value.to_string();
            Ok(new_value)
        } else {
            self.map
                .upsert(key, Expirable::new_perpetual("1".to_string()));
            Ok(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::Engine;

    struct FakeClock {
        now: std::cell::Cell<std::time::Instant>,
    }

    impl FakeClock {
        fn new(instant: std::time::Instant) -> Self {
            FakeClock {
                now: std::cell::Cell::new(instant),
            }
        }

        fn new_now() -> Self {
            FakeClock::new(std::time::Instant::now())
        }

        fn advance(&self, duration: std::time::Duration) {
            self.now.set(self.now.get() + duration);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> std::time::Instant {
            self.now.get()
        }
    }

    #[test]
    fn test_set_and_get() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Get {
            key: redis::Key("nonexistent".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_expiration_seconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_secs(1));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_expiration_milliseconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Milliseconds(redis::Integer(500))),
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_millis(500));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_client() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Client);
        assert_eq!(result, redis::Result::Ok);
    }

    #[test]
    fn test_incr() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Incr {
            key: redis::Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(1));

        let result = redis.call(redis::Command::Incr {
            key: redis::Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(2));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("2".to_string()));
    }
}
