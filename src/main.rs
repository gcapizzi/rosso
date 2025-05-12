use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::{Result, anyhow};

mod resp;

fn main() -> Result<()> {
    let mut db: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    for stream in listener.incoming() {
        handle_client(&mut db, stream?)?;
    }
    Ok(())
}

fn handle_client(
    db: &mut std::collections::HashMap<String, String>,
    stream: TcpStream,
) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    while has_data_left(&mut reader)? {
        let command = resp::parse(&mut reader)?;
        let reply = run_command(db, command)?;
        resp::serialise(&mut writer, &reply)?;
        writer.flush()?;
    }
    Ok(())
}

fn has_data_left<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().map(|b| !b.is_empty())
}

fn run_command(
    db: &mut std::collections::HashMap<String, String>,
    command: resp::Value,
) -> Result<resp::Value> {
    let mut cmd = to_vec_of_strings(command)?;
    let cmd_name = cmd.pop_front().ok_or(anyhow!("command is empty"))?;
    match cmd_name.as_str() {
        "GET" => {
            let key = cmd
                .pop_front()
                .ok_or(anyhow!("GET command requires a key"))?;
            Ok(db
                .get(&key)
                .map(|v| resp::Value::BulkString(v.clone()))
                .unwrap_or(resp::Value::Null))
        }
        "SET" => {
            let key = cmd
                .pop_front()
                .ok_or(anyhow!("SET command requires a key"))?;
            let value = cmd
                .pop_front()
                .ok_or(anyhow!("SET command requires a value"))?;
            db.insert(key, value);
            Ok(resp::Value::SimpleString("OK".to_string()))
        }
        "CLIENT" => Ok(resp::Value::SimpleString("OK".to_string())),
        _ => {
            return Err(anyhow!("unknown command: {}", cmd_name));
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
