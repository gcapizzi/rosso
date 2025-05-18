#[derive(Debug, PartialEq)]
pub enum Result {
    Null,
    Ok,
    BulkString(std::string::String),
}

#[derive(Debug, PartialEq)]
pub struct Key(pub std::string::String);

#[derive(Debug, PartialEq)]
pub struct String(pub std::string::String);

#[derive(Debug, PartialEq)]
pub enum Command {
    Get { key: Key },
    Set { key: Key, value: String },
    Client,
}
