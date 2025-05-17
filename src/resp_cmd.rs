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
            Ok(redis::Command::Set { key, value })
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
    }
}
