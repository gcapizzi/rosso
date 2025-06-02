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
pub enum Command {
    Get {
        key: Key,
    },
    Set {
        key: Key,
        value: String,
        ex: Option<Integer>,
    },
    Client,
    Incr {
        key: Key,
    },
}

pub trait Engine {
    fn call(&self, command: Command) -> Result;
}
