use std::net::SocketAddr;
use std::collections::{HashMap};
use futures::sync::mpsc;
use futures::{Future};

mod network;
mod codec;
mod paxos;

use network::Broker;
use paxos::Paxos;


fn main() {
    let local_id: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let peers_addr: HashMap<usize, SocketAddr> = vec![
        (0, "127.0.0.1:12345".parse().unwrap()),
        (1, "127.0.0.1:12346".parse().unwrap()),
        (2, "127.0.0.1:12347".parse().unwrap()),
        (3, "127.0.0.1:12348".parse().unwrap()),
    ].into_iter().collect();
    let peers_id = (0..peers_addr.len()).collect();
    let (itx, irx) = mpsc::unbounded();
    let (otx, orx) = mpsc::unbounded();
    let paxos = Paxos::new(peers_id, otx, irx);
    let broker = Broker::new(local_id, peers_addr, itx, orx);
    tokio::run(paxos.join(broker.map_err(|e|{dbg!(e);})).map(|_| {}));
}