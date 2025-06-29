#[derive(Debug, PartialEq)]
pub enum Result {
    Null,
    Ok,
    BulkString(std::string::String),
    Integer(i64),
    Error(std::string::String),
}

#[derive(Debug, PartialEq)]
pub struct Key(pub std::string::String);

#[derive(Debug, PartialEq)]
pub struct String(pub std::string::String);

#[derive(Debug, PartialEq)]
pub struct Integer(pub i64);

#[derive(Debug, PartialEq)]
pub enum Expiration {
    Seconds(Integer),
    Milliseconds(Integer),
    UnixTimeSeconds(Integer),
    UnixTimeMilliseconds(Integer),
    Keep,
}

#[derive(Debug, PartialEq)]
pub enum SetCondition {
    IfNotExists,
    IfExists,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Get {
        key: Key,
    },
    Set {
        key: Key,
        value: String,
        expiration: Option<Expiration>,
        get: bool,
        condition: Option<SetCondition>,
    },
    Client,
    Incr {
        key: Key,
    },
    Ttl {
        key: Key,
    },
    Append {
        key: Key,
        value: String,
    },
    Strlen {
        key: Key,
    },
}

pub trait Engine {
    fn call(&mut self, command: Command) -> Result;
}
