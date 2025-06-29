use async_net::{AsyncToSocketAddrs, TcpListener, TcpStream};
use smol::{
    LocalExecutor,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
};
use std::sync::{Arc, Mutex};

use crate::{engine, redis, resp, resp_cmd};

pub fn start<A: AsyncToSocketAddrs>(addr: A) -> std::io::Result<()> {
    let ex = LocalExecutor::new();
    smol::block_on(ex.run(async {
        let engine = engine::Default::new();
        let engine_pointer = Arc::new(Mutex::new(engine));
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let clone = engine_pointer.clone();
            ex.spawn(async move { handle_client(clone, socket).await.unwrap() })
                .detach();
        }
    }))
}

async fn handle_client<E: redis::Engine>(
    engine: Arc<Mutex<E>>,
    stream: TcpStream,
) -> std::io::Result<()> {
    // println!("Client connected: {}", stream.peer_addr()?);
    let mut reader = BufReader::new(stream.clone());
    let mut writer = BufWriter::new(stream.clone());

    while has_data_left(&mut reader).await? {
        let command = resp::parse(&mut reader).await?;
        // println!("Received command: {:?}", command);
        let reply = run_cmd(&mut *engine.lock().unwrap(), command);
        resp::serialise(&mut writer, &reply).await?;
        writer.flush().await?;
    }
    // println!("Client disconnected");
    Ok(())
}

fn run_cmd<E: redis::Engine>(engine: &mut E, command: resp::Value) -> resp::Value {
    resp_cmd::parse_command(command)
        .map(|cmd| engine.call(cmd))
        .map(|res| resp_cmd::serialise_result(res))
        .unwrap_or_else(|e| resp::Value::Error(format!("ERR {}", e)))
}

async fn has_data_left<R: AsyncBufRead + Unpin>(reader: &mut R) -> std::io::Result<bool> {
    reader.fill_buf().await.map(|b| !b.is_empty())
}
