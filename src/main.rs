use std::net::SocketAddr;
use std::collections::{HashMap};
use futures::channel::mpsc;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;
use network::Broker;
use paxos::{Paxos, Datagram, Request};

mod network;
mod paxos;


#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let server_num: usize = args.next().unwrap().parse().unwrap();
    let base_port: usize = args.next().map(|arg| arg.parse().unwrap()).unwrap_or(12345);
    let peers_addr: HashMap<usize, SocketAddr> = (base_port..(base_port+server_num))
        .enumerate()
        .map(|(id, port)| (id, format!("127.0.0.1:{}", port).parse().unwrap()))
        .collect();
    let peers_addr = Arc::new(peers_addr);
    let start_server = |id: usize| {
        let (itx, irx) = mpsc::unbounded();
        let (otx, orx) = mpsc::unbounded();
        let paxos = Paxos::new(id, (0..server_num).collect(), otx, irx);
        let broker = Broker::new(id, (*peers_addr).clone());
        tokio::spawn(broker.run(itx, orx));
        tokio::spawn(paxos.run());
    };
    (0..server_num).for_each(|id|{
        start_server(id);
    });
    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", base_port)).await {
        std::thread::sleep(std::time::Duration::from_secs(3));
        let dgram = Datagram::Request(Request::Propose{ value: 42 });
        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
    if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", base_port + 1)).await {
        std::thread::sleep(std::time::Duration::from_secs(1));
        let dgram = Datagram::Request(Request::Propose{ value: 24 });
        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
    }
    std::thread::sleep(std::time::Duration::from_secs(10));
}