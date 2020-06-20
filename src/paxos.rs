#![allow(unused)]
use std::collections::{HashSet};
use futures::channel::mpsc;
use bytes::{Bytes, BytesMut, BufMut};
use serde::{Serialize, Deserialize};
use tokio::stream::{Stream, StreamExt};

pub type Tx<T> = mpsc::UnboundedSender<T>;
pub type Rx<T> = mpsc::UnboundedReceiver<T>;

macro_rules! log {
    ($($tokens: tt)*) => {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        writeln!(handle, $($tokens)*).unwrap();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    Propose { value: u32 },
    Prepare { seq: usize },
    Accept { seq: usize, value: u32 },
    Learn { value: u32 },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Prepare { seq: usize, value: Option<u32> },
    Accept { seq: usize },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Datagram {
    Request(Request),
    Response(Response),
}

impl Datagram {

    pub fn encode_with_src(&self, src: usize) -> Bytes {
        const N: usize = std::mem::size_of::<usize>();

        let mut data = bincode::serialize(&self).unwrap();
        let mut buf = BytesMut::with_capacity(2 * N + data.len());

        buf.put_uint_be(src as u64, N);
        buf.put_uint_be(data.len() as u64, N);
        buf.put(data);
        buf.freeze()
    }
}

#[derive(Debug)]
pub struct Incoming {
    pub src: usize,
    pub dgram: Datagram,
}

#[derive(Debug)]
pub struct Outgoing {
    pub dst: HashSet<usize>,
    pub dgram: Datagram,
}

#[derive(Debug)]
struct Proposal {
    seq: usize,
    value: Option<u32>,
    wanted_value: u32,
    highest_seq: Option<usize>,
    prepared: HashSet<usize>,
    accepted: HashSet<usize>,
}


#[derive(Debug)]
pub struct Paxos {
    local_id: usize,
    peers_id: HashSet<usize>,
    last_promised: usize,
    chosen: Option<u32>,
    last_accepted: Option<u32>,
    // peers_num: usize,
    proposal: Option<Proposal>,
    current_seq: usize,
    tx: Tx<Outgoing>,
    rx: Rx<Incoming>
}

impl Paxos {
    pub fn new(local_id: usize, peers_id: HashSet<usize>, tx: Tx<Outgoing>, rx: Rx<Incoming>) -> Self {
        // log!("Paxos start with peers_num: {:?}", peers_id);
        Paxos {
            local_id,
            last_promised: 0,
            chosen: None,
            last_accepted: None,
            peers_id,
            proposal: None,
            current_seq: 0,
            tx,
            rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(incoming) = self.rx.next().await {
            self.handle_incoming(incoming);
        }
    }

    fn next_seq(&mut self) -> usize {
        self.current_seq += (self.local_id + 1);
        self.current_seq
    }

    fn handle_incoming(&mut self, incoming: Incoming) {
        let Incoming{src, dgram} = incoming;
        match dgram {
            Datagram::Request(req) => self.handle_request(src, req),
            Datagram::Response(resp) => self.handle_response(src, resp),
        }
    }

    fn handle_request(&mut self, src: usize, req: Request) {
        log!("Server #{} handle req: {:?} from #{}.", self.local_id, req, src);
        match req {
            Request::Prepare{seq} => {
                if self.last_promised <= seq {
                    let last_promised = self.last_promised;
                    self.last_promised = seq;
                    let resp = Response::Prepare{
                        seq: last_promised,
                        value: self.last_accepted,
                    };
                    self.tx.unbounded_send(Outgoing{
                        dst: (src..src+1).collect(),
                        dgram: Datagram::Response(resp),
                    }).unwrap();
                }
            },
            Request::Accept{seq, value} => {
                if self.last_promised <= seq {
                    self.last_accepted = Some(value);
                    let resp = Response::Accept{
                        seq: self.last_promised,
                    };
                    self.tx.unbounded_send(Outgoing{
                        dst: (src..src+1).collect(),
                        dgram: Datagram::Response(resp),
                    }).unwrap();
                }
            }
            Request::Learn{value} => {
                if let Some(chosen_value) = self.chosen {
                    assert!(chosen_value == value);
                }
                self.chosen = Some(value);
                log!("Server#{} learned {}", self.local_id, self.chosen.unwrap());
            }
            Request::Propose{value} => {
                if let Some(ref _proposal) = self.proposal {
                    log!("override an existed proposal.");
                }
                let seq = self.next_seq();
                self.proposal = Some(Proposal{
                    seq,
                    value: None,
                    wanted_value: value,
                    highest_seq: None,
                    prepared: HashSet::new(),
                    accepted: HashSet::new()
                });
                let req = Request::Prepare{ seq: seq };
                self.tx.unbounded_send(Outgoing{
                    dst: self.peers_id.clone(),
                    dgram: Datagram::Request(req),
                }).unwrap();
            }
            
        }
    }

    fn handle_response(&mut self, src: usize, resp: Response) {
        log!("Server #{} handle resp: {:?} from #{}.", self.local_id, resp, src);
        match resp {
            Response::Prepare{seq, value} => {
                if let Some(ref mut proposal) = self.proposal {
                    proposal.prepared.insert(src);
                    if proposal.prepared.len() <= self.peers_id.len() / 2 + 1
                        && value.is_some() && seq >= *proposal.highest_seq.get_or_insert(seq)
                    {
                        proposal.value = value;
                    }
                    if proposal.prepared.len() == self.peers_id.len() / 2 + 1 {
                        let req = Request::Accept{
                            seq: proposal.seq,
                            value: *proposal.value.get_or_insert(proposal.wanted_value),
                        };
                        self.tx.unbounded_send(Outgoing{
                            dst: proposal.prepared.clone(),
                            dgram: Datagram::Request(req)
                        }).unwrap();
                    }
                }
                else {
                    panic!("recv a resp for prepare, but no proposal presented");
                }
            },
            Response::Accept{seq} => {
                // log!("handle accept resp seq: {}", seq);
                if let Some(ref mut proposal) = self.proposal {
                    if seq == proposal.seq {
                        proposal.accepted.insert(src);
                        if proposal.accepted.len() == 1 + self.peers_id.len() / 2 {
                            assert!(proposal.value.is_some());
                            let value = proposal.value.unwrap();
                            if value == proposal.wanted_value {
                                log!("proposal value `{}` success.", value);
                            }
                            else {
                                log!("proposal value `{}` fail, `{}` is chosen.",
                                    proposal.wanted_value, value);
                            }
                            let req = Request::Learn{
                                value: proposal.value.unwrap()
                            };
                            log!("value accepted by majority: {}", value);
                            self.tx.unbounded_send(Outgoing{
                                dst: self.peers_id.clone(),
                                dgram: Datagram::Request(req)
                            }).unwrap();
                        }
                    }
                }
                else {
                    panic!("recv an accepted response, but no proposal presented")
                }
            }
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encode() {
        let dgram = Datagram::Request(Request::Propose{value: 42});
        let encoded = dgram.encode();
        let decoded: Datagram = bincode::deserialize(&encoded[8..]).unwrap();
        if let Datagram::Request(Request::Propose{ value: 42 }) = decoded {
            //..
        }
        else{
            panic!();
        }
    }

}
