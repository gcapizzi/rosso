use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::Result;

mod resp;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    for stream in listener.incoming() {
        handle_client(stream?)?;
    }
    Ok(())
}

fn handle_client(stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    while has_data_left(&mut reader)? {
        let command = resp::parse(&mut reader)?;
        let reply = run_command(command)?;
        resp::serialise(&mut writer, &reply)?;
        writer.flush()?;
    }
    Ok(())
}

fn has_data_left<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().map(|b| !b.is_empty())
}

fn run_command(command: resp::Value) -> std::io::Result<resp::Value> {
    let cmd = to_vec_of_strings(command)?;
    if cmd[0] == "GET" {
        Ok(resp::Value::Integer(42))
    } else {
        Ok(resp::Value::SimpleString("OK".to_string()))
    }
}

fn to_vec_of_strings(value: resp::Value) -> std::io::Result<Vec<String>> {
    if let resp::Value::Array(values) = value {
        values
            .into_iter()
            .map(|v| {
                if let resp::Value::BulkString(s) = v {
                    Ok(s)
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid command",
                    ));
                }
            })
            .collect::<std::io::Result<Vec<_>>>()
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid command",
        ))
    }
}
