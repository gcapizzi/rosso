use std::collections::{HashMap, VecDeque};

use crate::resp;
use anyhow::{Result, anyhow};

pub struct Redis {
    map: HashMap<String, String>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            map: HashMap::new(),
        }
    }

    pub fn call(&mut self, command: resp::Value) -> resp::Value {
        self.run_command(command)
            .unwrap_or_else(|e| resp::Value::Error(format!("ERR {}", e)))
    }

    fn run_command(&mut self, command: resp::Value) -> Result<resp::Value> {
        let mut cmd = to_vec_of_strings(command)?;
        let cmd_name = cmd.pop_front().ok_or(anyhow!("command is empty"))?;
        match cmd_name.as_str() {
            "GET" => {
                let key = cmd
                    .pop_front()
                    .ok_or(anyhow!("wrong number of arguments for 'get' command"))?;
                let value = self
                    .map
                    .get(&key)
                    .map(|v| resp::Value::BulkString(v.clone()))
                    .unwrap_or(resp::Value::Null);
                Ok(value)
            }
            "SET" => {
                let key = cmd
                    .pop_front()
                    .ok_or(anyhow!("wrong number of arguments for 'set' command"))?;
                let value = cmd
                    .pop_front()
                    .ok_or(anyhow!("wrong number of arguments for 'set' command"))?;
                self.map.insert(key, value);
                Ok(resp::Value::SimpleString("OK".to_string()))
            }
            "CLIENT" => Ok(resp::Value::SimpleString("OK".to_string())),
            _ => {
                return Err(anyhow!("unknown command '{}'", cmd_name));
            }
        }
    }
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
