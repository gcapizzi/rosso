use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
};

use anyhow::Result;

mod hashmap;
mod resp;

fn main() -> Result<()> {
    let mut redis: hashmap::Redis = hashmap::Redis::new();
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    for stream in listener.incoming() {
        handle_client(&mut redis, stream?)?;
    }
    Ok(())
}

fn handle_client(redis: &mut hashmap::Redis, stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    while has_data_left(&mut reader)? {
        let command = resp::parse(&mut reader)?;
        let reply = redis.call(command);
        resp::serialise(&mut writer, &reply)?;
        writer.flush()?;
    }
    Ok(())
}

fn has_data_left<R: BufRead>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().map(|b| !b.is_empty())
}
