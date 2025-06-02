use std::collections::VecDeque;

use crate::redis;
use crate::resp;
use anyhow::{Result, anyhow};

pub fn parse_command(command: resp::Value) -> Result<redis::Command> {
    let mut cmd = to_vec_of_strings(command)?;
    let cmd_name = cmd.pop_front().ok_or(anyhow!("command is empty"))?;
    match cmd_name.as_str() {
        "GET" => {
            let key = consume_key("get", &mut cmd)?;
            Ok(redis::Command::Get { key })
        }
        "SET" => {
            let key = consume_key("set", &mut cmd)?;
            let value = consume_string("set", &mut cmd)?;
            let mut ex = None;
            if let Some(kw) = cmd.pop_front() {
                if kw == "EX" {
                    ex = Some(consume_integer("set", &mut cmd)?);
                } else {
                    return Err(anyhow!("unknown argument '{}' for SET command", kw));
                }
            }

            Ok(redis::Command::Set { key, value, ex })
        }
        "INCR" => {
            let key = consume_key("incr", &mut cmd)?;
            Ok(redis::Command::Incr { key })
        }
        "CLIENT" => Ok(redis::Command::Client),
        _ => {
            return Err(anyhow!("unknown command '{}'", cmd_name));
        }
    }
}

fn consume_arg(cmd_name: &str, args: &mut VecDeque<String>) -> Result<String> {
    args.pop_front().ok_or(anyhow!(
        "wrong number of arguments for '{}' command",
        cmd_name
    ))
}

fn consume_key(cmd_name: &str, args: &mut VecDeque<String>) -> Result<redis::Key> {
    consume_arg(cmd_name, args).map(|v| redis::Key(v))
}

fn consume_string(cmd_name: &str, args: &mut VecDeque<String>) -> Result<redis::String> {
    consume_arg(cmd_name, args).map(|v| redis::String(v))
}

fn consume_integer(cmd_name: &str, args: &mut VecDeque<String>) -> Result<redis::Integer> {
    consume_arg(cmd_name, args)
        .and_then(|v| {
            v.parse()
                .map_err(|_| anyhow!("invalid EX value for '{}' command: '{}'", cmd_name, v))
        })
        .map(|v| redis::Integer(v))
}

fn to_vec_of_strings(value: resp::Value) -> Result<VecDeque<String>> {
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
                ex: None,
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
                ex: Some(Integer(3)),
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
            "wrong number of arguments for 'get' command"
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
