pub enum Result {
    Null,
    Ok,
    BulkString(std::string::String),
}

pub struct Key(pub std::string::String);
pub struct String(pub std::string::String);

pub enum Command {
    Get { key: Key },
    Set { key: Key, value: String },
    Client,
}
