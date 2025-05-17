use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::Result;

fn main() -> Result<()> {
    let mut redis: rosso::hashmap::Redis = rosso::hashmap::Redis::new();
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    for stream in listener.incoming() {
        handle_client(&mut redis, stream?)?;
    }
    Ok(())
}

fn handle_client(redis: &mut rosso::hashmap::Redis, stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    while has_data_left(&mut reader)? {
        let command = rosso::resp::parse(&mut reader)?;
        let reply = run_cmd(redis, command);
        rosso::resp::serialise(&mut writer, &reply)?;
        writer.flush()?;
    }
    Ok(())
}

fn run_cmd(redis: &mut rosso::hashmap::Redis, command: rosso::resp::Value) -> rosso::resp::Value {
    rosso::resp_cmd::parse_command(command)
        .map(|cmd| redis.call(cmd))
        .map(|res| rosso::resp_cmd::serialise_result(res))
        .unwrap_or_else(|e| rosso::resp::Value::Error(format!("ERR {}", e)))
}

fn has_data_left<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().map(|b| !b.is_empty())
}
