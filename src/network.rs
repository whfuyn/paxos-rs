use std::net::SocketAddr;
use std::collections::{HashMap};
use futures::sync::mpsc;
use tokio::net::{UdpSocket, UdpFramed};
use futures::{Future, Sink, Stream, Poll, Async};

use crate::*;
use crate::paxos::*;
use crate::codec::Codec;

use tokio::prelude::stream::{SplitSink, SplitStream};
use futures::sink::SendAll;
use futures::stream::Forward;
use futures::future::Join;
use codec::FlowError;


pub type Package = (Datagram, SocketAddr);
type Inflow = Forward<SplitStream<UdpFramed<Codec>>, Tx<Package>>;
type Outflow = SendAll<SplitSink<UdpFramed<Codec>>, Rx<Package>>;

#[derive(Debug)]
pub struct Broker {
    local_id: usize,
    peers_addr: HashMap<usize, SocketAddr>,
    addr_id: HashMap<SocketAddr, usize>,
    itx: Tx<Incoming>,
    irx: Rx<Outgoing>,
    otx: Tx<Package>,
    orx: Rx<Package>,
    flow: Join<Inflow, Outflow>,
}


impl Broker {
    pub fn new(
        local_id: usize,
        peers_addr: HashMap<usize, SocketAddr>,
        itx: Tx<Incoming>,
        irx: Rx<Outgoing>) -> Self 
    {
        let local_addr = peers_addr[&local_id];
        let socket = UdpSocket::bind(&local_addr).unwrap();
        let addr_id: HashMap<SocketAddr, usize> = peers_addr.iter().map(|(&k, &v)| (v, k)).collect();

        // inflow : udp_rx -> from_udp_tx -> orx -> (...)
        // outflow: udp_tx <-   to_udp_rx <- otx <- (...)
        let framed = UdpFramed::new(socket, Codec::new());
        let (udp_tx, udp_rx) = framed.split();
        let (otx, to_udp_rx) = mpsc::unbounded();
        let (from_udp_tx, orx) = mpsc::unbounded();

        let inflow = udp_rx.forward(from_udp_tx);
        let outflow = udp_tx.send_all(to_udp_rx);
        let flow = inflow.join(outflow);

        Broker {
            local_id,
            peers_addr,
            addr_id,
            itx,
            irx,
            otx,
            orx,
            flow,
        }
    }
    
}

impl Future for Broker {
    type Item = ();
    type Error = FlowError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // println!("broker polled");
        loop {
            {
                static mut DONE: bool = false;
                unsafe {
                    if !DONE && self.local_id == 0 {
                        DONE = true;
                        let dgram = Datagram::Request(Request::Propose{value: 42});
                        self.itx.unbounded_send(Incoming{
                            src: self.local_id,
                            dgram,
                        }).unwrap();
                    }
                }
            }
            let s1 = match self.orx.poll()? {
                Async::Ready(Some((dgram, socket))) => {
                    println!("recv {:?} {:?}", dgram, socket);
                    self.itx.unbounded_send(Incoming{
                        src: self.addr_id[&socket],
                        dgram,
                    }).unwrap();
                    Async::Ready(Some(()))
                }
                Async::Ready(None) => Async::Ready(None),
                Async::NotReady => Async::NotReady,
            };
            let s2 = match self.irx.poll()? {
                Async::Ready(Some(Outgoing{dst, dgram})) => {
                    println!("send {:?} {:?}", dst, dgram);
                    dst.iter().for_each(|id|{
                        let socket = self.peers_addr[&id];
                        self.otx.unbounded_send((dgram.clone(), socket)).unwrap();
                    });
                    Async::Ready(Some(()))
                }
                Async::Ready(None) => Async::Ready(None),
                Async::NotReady => Async::NotReady,
            };
            let s0 = self.flow.poll()?;
            match (s0, s1, s2) {
                (Async::Ready(_), Async::Ready(None), Async::Ready(None)) => return Ok(Async::Ready(())),
                (Async::NotReady, Async::NotReady, Async::NotReady) => return Ok(Async::NotReady),
                (_, _, _) => continue,
            }
        }
    }

}

// #[derive(Debug)]
// struct Peer {
//     rx: Rx<(Datagram, SocketAddr)>,
//     tx: Tx<(Datagram, SocketAddr)>,
//     framed_sink: SplitSink<UdpFramed<Codec>>,
//     framed_stream: SplitStream<UdpFramed<Codec>>,
// }



// #[derive(Debug)]
// struct Peer {
//     // inflow: Forward<SplitStream<UdpFramed<Codec>>, Tx<(Datagram, SocketAddr)>>,
//     // outflow: SendAll<SplitSink<UdpFramed<Codec>>, Rx<(Datagram, SocketAddr)>>,
//     flow: Join<Inflow, Outflow>,
// }

// impl Peer {
//     fn new(rx: Rx<Package>, tx: Tx<Package>, framed: UdpFramed<Codec>) -> Self {
//         let (sink, stream) = framed.split();
//         let inflow = stream.forward(tx);
//         let outflow = sink.send_all(rx);
//         Peer {
//             flow: inflow.join(outflow)
//         }
//     }
// }

// impl Future for Peer {
//     type Item = ();
//     type Error = FlowError;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         // dbg!("Peer polled");
//         try_ready!(self.flow.poll());
//         Ok(Async::Ready(()))
//     }


// }

