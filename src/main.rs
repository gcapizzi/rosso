use async_net::{TcpListener, TcpStream};
use macro_rules_attribute::apply;
use smol::{
    LocalExecutor,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
};
use smol_macros::main;
use std::sync::{Arc, Mutex};

use anyhow::Result;

#[apply(main!)]
async fn main(ex: &LocalExecutor<'_>) -> Result<()> {
    let redis = Arc::new(Mutex::new(rosso::hashmap::Redis::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        let clone = redis.clone();
        ex.spawn(async move { handle_client(clone, socket).await.unwrap() })
            .detach();
    }
}

async fn handle_client(redis: Arc<Mutex<rosso::hashmap::Redis>>, stream: TcpStream) -> Result<()> {
    println!("Client connected: {}", stream.peer_addr()?);
    let mut reader = BufReader::new(stream.clone());
    let mut writer = BufWriter::new(stream.clone());

    while has_data_left(&mut reader).await? {
        let command = rosso::resp::parse(&mut reader).await?;
        println!("Received command: {:?}", command);
        let reply = run_cmd(&redis, command);
        rosso::resp::serialise(&mut writer, &reply).await?;
        writer.flush().await?;
    }
    println!("Client disconnected");
    Ok(())
}

fn run_cmd(
    redis: &Mutex<rosso::hashmap::Redis>,
    command: rosso::resp::Value,
) -> rosso::resp::Value {
    let mut redis = redis.lock().unwrap();
    rosso::resp_cmd::parse_command(command)
        .map(|cmd| redis.call(cmd))
        .map(|res| rosso::resp_cmd::serialise_result(res))
        .unwrap_or_else(|e| rosso::resp::Value::Error(format!("ERR {}", e)))
}

async fn has_data_left<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().await.map(|b| !b.is_empty())
}
