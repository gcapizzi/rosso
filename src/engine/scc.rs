use crate::redis;

#[derive(Debug)]
struct Expirable<T> {
    pub value: T,
    expires_at: Option<std::time::SystemTime>,
}

impl<T> Expirable<T> {
    pub fn new(value: T, expires_at: Option<std::time::SystemTime>) -> Self {
        Expirable { value, expires_at }
    }

    pub fn new_perpetual(value: T) -> Self {
        Self::new(value, None)
    }

    fn is_expired(&self, now: std::time::SystemTime) -> bool {
        self.expires_at.map_or(false, |t| t <= now)
    }
}

pub trait Clock {
    fn now(&self) -> std::time::SystemTime;
}

pub struct StdClock;

impl Clock for StdClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
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
                .read(&k, |e| e.value.to_string())
                .map(|v| redis::Result::BulkString(v))
                .unwrap_or(redis::Result::Null),
            redis::Command::Set {
                key: redis::Key(k),
                value: redis::String(v),
                expiration,
                get,
                condition,
            } => {
                let entry = self.entry(k);
                let ex = expiration.as_ref().and_then(|e| match e {
                    redis::Expiration::Seconds(redis::Integer(secs)) => {
                        Some(self.clock.now() + std::time::Duration::from_secs(*secs as u64))
                    }
                    redis::Expiration::Milliseconds(redis::Integer(millis)) => {
                        Some(self.clock.now() + std::time::Duration::from_millis(*millis as u64))
                    }
                    redis::Expiration::UnixTimeSeconds(redis::Integer(secs)) => Some(
                        std::time::SystemTime::UNIX_EPOCH
                            + std::time::Duration::from_secs(*secs as u64),
                    ),
                    redis::Expiration::UnixTimeMilliseconds(redis::Integer(millis)) => Some(
                        std::time::SystemTime::UNIX_EPOCH
                            + std::time::Duration::from_millis(*millis as u64),
                    ),
                    redis::Expiration::Keep => None,
                });
                match entry {
                    scc::hash_map::Entry::Occupied(mut e) => {
                        if condition
                            .as_ref()
                            .is_some_and(|c| c == &redis::SetCondition::IfNotExists)
                        {
                            return redis::Result::Null;
                        }
                        let ex = if let Some(redis::Expiration::Keep) = expiration {
                            e.expires_at
                        } else {
                            ex
                        };
                        let pv = std::mem::replace(e.get_mut(), Expirable::new(v, ex)).value;
                        if get {
                            redis::Result::BulkString(pv)
                        } else {
                            redis::Result::Ok
                        }
                    }
                    scc::hash_map::Entry::Vacant(e) => {
                        if condition
                            .as_ref()
                            .is_some_and(|c| c == &redis::SetCondition::IfExists)
                        {
                            return redis::Result::Null;
                        }
                        e.insert_entry(Expirable::new(v, ex));
                        if get {
                            redis::Result::Null
                        } else {
                            redis::Result::Ok
                        }
                    }
                }
            }
            redis::Command::Client => redis::Result::Ok,
            redis::Command::Incr { key: redis::Key(k) } => match self.entry(k) {
                scc::hash_map::Entry::Occupied(mut e) => e
                    .value
                    .parse()
                    .and_then(|v: i64| {
                        let nv = v + 1;
                        e.value = nv.to_string();
                        Ok(redis::Result::Integer(nv))
                    })
                    .unwrap_or_else(|e| redis::Result::Error(e.to_string())),
                scc::hash_map::Entry::Vacant(e) => {
                    e.insert_entry(Expirable::new_perpetual("1".to_string()));
                    redis::Result::Integer(1)
                }
            },
            redis::Command::Ttl { key: redis::Key(k) } => redis::Result::Integer({
                self.read(&k, |e| {
                    e.expires_at.map_or(-1, |t| {
                        t.duration_since(self.clock.now())
                            .map_or(-2, |d| d.as_secs() as i64)
                    })
                })
                .unwrap_or(-2)
            }),
            redis::Command::Append {
                key: redis::Key(k),
                value: redis::String(v),
            } => redis::Result::Integer({
                match self.entry(k) {
                    scc::hash_map::Entry::Occupied(mut e) => {
                        e.value.push_str(&v);
                        e.value.len() as i64
                    }
                    scc::hash_map::Entry::Vacant(e) => {
                        let len = v.len();
                        e.insert_entry(Expirable::new_perpetual(v));
                        len as i64
                    }
                }
            }),
            redis::Command::Strlen { key: redis::Key(k) } => {
                redis::Result::Integer(self.read(&k, |e| e.value.len() as i64).unwrap_or(0))
            }
        }
    }
}

impl<C: Clock> ConcurrentHashMap<'_, C> {
    fn read<T, R: FnOnce(&Expirable<String>) -> T>(&self, key: &str, reader: R) -> Option<T> {
        self.map.remove_if(key, |e| e.is_expired(self.clock.now()));
        self.map.read(key, |_, e| reader(e))
    }

    fn entry(
        &self,
        key: String,
    ) -> scc::hash_map::Entry<'_, std::string::String, Expirable<std::string::String>> {
        self.map.remove_if(&key, |e| e.is_expired(self.clock.now()));
        self.map.entry(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::Engine;

    struct FakeClock {
        now: std::cell::Cell<std::time::SystemTime>,
    }

    impl FakeClock {
        fn new(time: std::time::SystemTime) -> Self {
            FakeClock {
                now: std::cell::Cell::new(time),
            }
        }

        fn new_now() -> Self {
            FakeClock::new(std::time::SystemTime::now())
        }

        fn advance(&self, duration: std::time::Duration) {
            self.now.set(self.now.get() + duration);
        }

        fn set(&self, time: std::time::SystemTime) {
            self.now.set(time);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> std::time::SystemTime {
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
            get: false,
            condition: None,
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
    fn test_set_expiration_seconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_secs(1));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_expiration_milliseconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Milliseconds(redis::Integer(500))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_millis(500));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_expiration_unix_time_seconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::UnixTimeSeconds(redis::Integer(
                1749371595,
            ))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock.set(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1749371596));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_expiration_unix_time_milliseconds() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::UnixTimeMilliseconds(redis::Integer(
                1749371595123,
            ))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock
            .set(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1749371595124));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_expiration_keep() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Keep),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_secs(1));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_expiration_reset() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        clock.advance(std::time::Duration::from_secs(1));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));
    }

    #[test]
    fn test_set_get() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
            get: true,
            condition: None,
        });
        assert_eq!(result, redis::Result::Null);
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("new_value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: true,
            condition: None,
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("newer_value".to_string()),
            expiration: None,
            get: true,
            condition: None,
        });
        assert_eq!(result, redis::Result::Null);
    }

    #[test]
    fn test_set_if_not_exists() {
        let redis = ConcurrentHashMap::new();

        // key does not exist
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
            get: false,
            condition: Some(redis::SetCondition::IfNotExists),
        });
        assert_eq!(result, redis::Result::Ok);

        // key exists
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("new_value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: false,
            condition: Some(redis::SetCondition::IfNotExists),
        });
        assert_eq!(result, redis::Result::Null);
        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("value".to_string()));

        // key exists, but it's expired
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("new_value".to_string()),
            expiration: None,
            get: false,
            condition: Some(redis::SetCondition::IfNotExists),
        });
        assert_eq!(result, redis::Result::Ok);
        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("new_value".to_string()));
    }

    #[test]
    fn test_set_if_exists() {
        let redis = ConcurrentHashMap::new();

        // key does not exist
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
            get: false,
            condition: Some(redis::SetCondition::IfExists),
        });
        assert_eq!(result, redis::Result::Null);
        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Null);

        // key exists
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: None,
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("new_value".to_string()),
            expiration: None,
            get: false,
            condition: Some(redis::SetCondition::IfExists),
        });
        assert_eq!(result, redis::Result::Ok);
        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("new_value".to_string()));

        // key exists, but it's expired
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("value".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);
        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("new_value".to_string()),
            expiration: None,
            get: false,
            condition: Some(redis::SetCondition::IfExists),
        });
        assert_eq!(result, redis::Result::Null);
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
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Incr {
            key: redis::Key("counter".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(1));

        let result = redis.call(redis::Command::Set {
            key: redis::Key("counter".to_string()),
            value: redis::String("42".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

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

    #[test]
    fn test_ttl() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("foo".to_string()),
            value: redis::String("42".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let ttl = redis.call(redis::Command::Ttl {
            key: redis::Key("foo".to_string()),
        });
        assert_eq!(ttl, redis::Result::Integer(1));

        clock.advance(std::time::Duration::from_millis(500));
        let ttl = redis.call(redis::Command::Ttl {
            key: redis::Key("foo".to_string()),
        });
        assert_eq!(ttl, redis::Result::Integer(0));

        clock.advance(std::time::Duration::from_millis(500));
        let ttl = redis.call(redis::Command::Ttl {
            key: redis::Key("foo".to_string()),
        });
        assert_eq!(ttl, redis::Result::Integer(-2));
    }

    #[test]
    fn test_no_ttl() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Set {
            key: redis::Key("foo".to_string()),
            value: redis::String("42".to_string()),
            expiration: None,
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let ttl = redis.call(redis::Command::Ttl {
            key: redis::Key("foo".to_string()),
        });
        assert_eq!(ttl, redis::Result::Integer(-1));
    }

    #[test]
    fn test_append() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Append {
            key: redis::Key("key".to_string()),
            value: redis::String("hello".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(5));

        let result = redis.call(redis::Command::Append {
            key: redis::Key("key".to_string()),
            value: redis::String(", world!".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(13));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(
            result,
            redis::Result::BulkString("hello, world!".to_string())
        );
    }

    #[test]
    fn test_append_to_expired_key() {
        let redis = ConcurrentHashMap::new();

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("bye!".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(0))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Append {
            key: redis::Key("key".to_string()),
            value: redis::String("hello!".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(6));

        let result = redis.call(redis::Command::Get {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::BulkString("hello!".to_string()));
    }

    #[test]
    fn test_strlen() {
        let clock = FakeClock::new_now();
        let redis = ConcurrentHashMap::with_clock(&clock);

        let result = redis.call(redis::Command::Strlen {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(0));

        let result = redis.call(redis::Command::Set {
            key: redis::Key("key".to_string()),
            value: redis::String("hello, world!".to_string()),
            expiration: Some(redis::Expiration::Seconds(redis::Integer(1))),
            get: false,
            condition: None,
        });
        assert_eq!(result, redis::Result::Ok);

        let result = redis.call(redis::Command::Strlen {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(13));

        clock.advance(std::time::Duration::from_secs(1));

        let result = redis.call(redis::Command::Strlen {
            key: redis::Key("key".to_string()),
        });
        assert_eq!(result, redis::Result::Integer(0));
    }
}
