use std::collections::VecDeque;

use crate::redis;
use crate::resp;
use anyhow::{Result, anyhow};

pub fn parse_command(command: resp::Value) -> Result<redis::Command> {
    let mut cmd = to_vec(command)?;
    let cmd_name = cmd.pop_front().ok_or(anyhow!("command is empty"))?;
    match cmd_name.as_str() {
        "GET" => get(&mut cmd),
        "SET" => set(&mut cmd),
        "INCR" => incr(&mut cmd),
        "TTL" => ttl(&mut cmd),
        "APPEND" => append(&mut cmd),
        "STRLEN" => strlen(&mut cmd),
        "CLIENT" => Ok(redis::Command::Client),
        _ => {
            return Err(anyhow!("unknown command '{}'", cmd_name));
        }
    }
}

fn get(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    Ok(redis::Command::Get { key })
}

fn set(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    let value = string(args)?;
    let mut expiration = None;
    let mut get = false;
    let mut condition = None;
    while let Some(arg) = args.pop_front() {
        match arg.as_str() {
            "EX" => {
                expiration = Some(redis::Expiration::Seconds(integer(args)?));
            }
            "PX" => {
                expiration = Some(redis::Expiration::Milliseconds(integer(args)?));
            }
            "EXAT" => {
                expiration = Some(redis::Expiration::UnixTimeSeconds(integer(args)?));
            }
            "PXAT" => {
                expiration = Some(redis::Expiration::UnixTimeMilliseconds(integer(args)?));
            }
            "KEEPTTL" => {
                expiration = Some(redis::Expiration::Keep);
            }
            "GET" => {
                get = true;
            }
            "NX" => {
                condition = Some(redis::SetCondition::IfNotExists);
            }
            "XX" => {
                condition = Some(redis::SetCondition::IfExists);
            }
            _ => {
                return Err(anyhow!("unexpected argument '{}'", arg));
            }
        }
    }
    Ok(redis::Command::Set {
        key,
        value,
        expiration,
        get,
        condition,
    })
}

fn incr(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    Ok(redis::Command::Incr { key })
}

fn ttl(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    Ok(redis::Command::Ttl { key })
}

fn append(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    let value = string(args)?;
    Ok(redis::Command::Append { key, value })
}

fn strlen(args: &mut VecDeque<String>) -> Result<redis::Command> {
    let key = key(args)?;
    Ok(redis::Command::Strlen { key })
}

fn arg(args: &mut VecDeque<String>) -> Result<String> {
    args.pop_front().ok_or(anyhow!("wrong number of arguments"))
}

fn key(args: &mut VecDeque<String>) -> Result<redis::Key> {
    arg(args).map(|v| redis::Key(v))
}

fn string(args: &mut VecDeque<String>) -> Result<redis::String> {
    arg(args).map(|v| redis::String(v))
}

fn integer(args: &mut VecDeque<String>) -> Result<redis::Integer> {
    arg(args)
        .and_then(|v| v.parse().map_err(|_| anyhow!("not an integer: {}", v)))
        .map(|v| redis::Integer(v))
}

fn to_vec(value: resp::Value) -> Result<VecDeque<String>> {
    if let resp::Value::Array(values) = value {
        values
            .into_iter()
            .map(|v| {
                if let resp::Value::BulkString(s) = v {
                    Ok(s)
                } else {
                    return Err(anyhow!(
                        "invalid command: it should be an array of bulk strings",
                    ));
                }
            })
            .collect::<Result<VecDeque<_>>>()
    } else {
        Err(anyhow!("invalid command: it should be an array",))
    }
}

pub fn serialise_result(result: redis::Result) -> resp::Value {
    match result {
        redis::Result::BulkString(s) => resp::Value::BulkString(s),
        redis::Result::Null => resp::Value::Null,
        redis::Result::Ok => resp::Value::SimpleString("OK".to_string()),
        redis::Result::Integer(n) => resp::Value::Integer(n),
        redis::Result::Error(e) => resp::Value::Error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis::*;

    #[test]
    fn test_parse_command_get() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("GET".to_string()),
            resp::Value::BulkString("key".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Get {
                key: Key("key".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_command_set() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: None,
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_ex() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("EX".to_string()),
            resp::Value::BulkString("3".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: Some(Expiration::Seconds(Integer(3))),
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_px() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("PX".to_string()),
            resp::Value::BulkString("300".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: Some(Expiration::Milliseconds(Integer(300))),
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_exat() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("EXAT".to_string()),
            resp::Value::BulkString("1749371595".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: Some(Expiration::UnixTimeSeconds(Integer(1749371595))),
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_pxat() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("PXAT".to_string()),
            resp::Value::BulkString("1749371595123".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: Some(Expiration::UnixTimeMilliseconds(Integer(1749371595123))),
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_keepttl() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("KEEPTTL".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: Some(Expiration::Keep),
                get: false,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_get() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("GET".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: None,
                get: true,
                condition: None,
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_nx() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("NX".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: None,
                get: false,
                condition: Some(redis::SetCondition::IfNotExists),
            }
        );
    }

    #[test]
    fn test_parse_command_set_with_xx() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("SET".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
            resp::Value::BulkString("XX".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Set {
                key: Key("key".to_string()),
                value: String("value".to_string()),
                expiration: None,
                get: false,
                condition: Some(redis::SetCondition::IfExists),
            }
        );
    }

    #[test]
    fn test_parse_command_client() {
        let command = resp::Value::Array(vec![resp::Value::BulkString("CLIENT".to_string())]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(parsed_command, redis::Command::Client);
    }

    #[test]
    fn test_parse_command_incr() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("INCR".to_string()),
            resp::Value::BulkString("key".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Incr {
                key: Key("key".to_string())
            }
        );
    }

    #[test]
    fn test_parse_command_ttl() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("TTL".to_string()),
            resp::Value::BulkString("key".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Ttl {
                key: Key("key".to_string())
            }
        );
    }

    #[test]
    fn test_parse_command_append() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("APPEND".to_string()),
            resp::Value::BulkString("key".to_string()),
            resp::Value::BulkString("value".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Append {
                key: Key("key".to_string()),
                value: String("value".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_command_strlen() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("STRLEN".to_string()),
            resp::Value::BulkString("key".to_string()),
        ]);
        let parsed_command = parse_command(command).unwrap();
        assert_eq!(
            parsed_command,
            redis::Command::Strlen {
                key: Key("key".to_string()),
            }
        );
    }

    #[test]
    fn test_parse_command_unknown() {
        let command = resp::Value::Array(vec![resp::Value::BulkString("UNKNOWN".to_string())]);
        let parsed_command = parse_command(command);
        assert!(parsed_command.is_err());
        assert_eq!(
            parsed_command.unwrap_err().to_string(),
            "unknown command 'UNKNOWN'"
        );
    }

    #[test]
    fn test_parse_command_not_enough_arguments() {
        let command = resp::Value::Array(vec![resp::Value::BulkString("GET".to_string())]);
        let parsed_command = parse_command(command);
        assert!(parsed_command.is_err());
        assert_eq!(
            parsed_command.unwrap_err().to_string(),
            "wrong number of arguments"
        );
    }

    #[test]
    fn test_parse_command_not_array() {
        let command = resp::Value::SimpleString("Hello".to_string());
        let parsed_command = parse_command(command);
        assert!(parsed_command.is_err());
        assert_eq!(
            parsed_command.unwrap_err().to_string(),
            "invalid command: it should be an array"
        );
    }

    #[test]
    fn test_parse_command_not_bulk_string_array() {
        let command = resp::Value::Array(vec![
            resp::Value::BulkString("GET".to_string()),
            resp::Value::SimpleString("key".to_string()),
        ]);
        let parsed_command = parse_command(command);
        assert!(parsed_command.is_err());
        assert_eq!(
            parsed_command.unwrap_err().to_string(),
            "invalid command: it should be an array of bulk strings"
        );
    }

    #[test]
    fn test_serialise_result_bulk_string() {
        let result = redis::Result::BulkString("Hello".to_string());
        let serialised = serialise_result(result);
        assert_eq!(serialised, resp::Value::BulkString("Hello".to_string()));
    }

    #[test]
    fn test_serialise_result_null() {
        let result = redis::Result::Null;
        let serialised = serialise_result(result);
        assert_eq!(serialised, resp::Value::Null);
    }

    #[test]
    fn test_serialise_result_ok() {
        let result = redis::Result::Ok;
        let serialised = serialise_result(result);
        assert_eq!(serialised, resp::Value::SimpleString("OK".to_string()));
    }
}
