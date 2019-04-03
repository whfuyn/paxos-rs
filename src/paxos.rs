use std::collections::{HashSet};
use futures::sync::mpsc;
use futures::{Future, Stream, Poll, Async};
use bytes::{BytesMut, BufMut};

pub type Tx<T> = mpsc::UnboundedSender<T>;
pub type Rx<T> = mpsc::UnboundedReceiver<T>;

#[derive(Debug, Clone)]
pub enum Request {
    Propose { value: u32 },
    Prepare { seq: usize },
    Accept { seq: usize, value: u32 },
    Learn { value: u32 },
}

impl Request {
    fn len(&self) -> usize {
        let n_usize = std::mem::size_of::<usize>();
        let n_u32 = std::mem::size_of::<u32>();
        match self {
            Request::Propose{ value: _ } => 1 + n_u32,
            Request::Prepare{ seq: _ } => 1 + n_usize,
            Request::Accept{ seq: _, value: _ } => 1 + n_usize + n_u32,
            Request::Learn{ value: _ } => 1 + n_u32,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Request::Propose{ value } => {
                buf.put_u8(0);
                buf.put_u32_be(*value);
                // buf.put_uint_be(*value as u64, std::mem::size_of::<usize>());
            }
            Request::Prepare{ seq } => {
                buf.put_u8(1);
                buf.put_uint_be(*seq as u64, std::mem::size_of::<usize>());
            }
            Request::Accept{ seq, value } => {
                buf.put_u8(2);
                buf.put_uint_be(*seq as u64, std::mem::size_of::<usize>());
                buf.put_u32_be(*value);
            }
            Request::Learn{ value } => {
                buf.put_u8(3);
                buf.put_u32_be(*value);
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Request {
        let ty = buf.split_to(1)[0];
        match ty {
            0 => {
                let value = get_u32_from_buf(buf);
                Request::Propose{ value }
            }
            1 => {
                let seq = get_usize_from_buf(buf);
                Request::Prepare{ seq }
            }
            2 => {
                let seq = get_usize_from_buf(buf);
                let value = get_u32_from_buf(buf);
                Request::Accept{ seq, value }
            }
            3 => {
                let value = get_u32_from_buf(buf);
                Request::Learn{ value }
            }
            _ => {
                panic!("found corrupt data while decoding to Request")
            }
        }
    }
}

fn get_u32_from_buf(buf: &mut BytesMut) -> u32 {
    const N: usize = std::mem::size_of::<u32>();
    let value = buf.split_to(N);
    let mut space = [0; N];
    space.clone_from_slice(&value[..]);
    u32::from_be_bytes(space)
}

fn get_usize_from_buf(buf: &mut BytesMut) -> usize {
    const N: usize = std::mem::size_of::<usize>();
    let value = buf.split_to(N);
    let mut space = [0; N];
    space.clone_from_slice(&value[..]);
    usize::from_be_bytes(space)
}

#[derive(Debug, Clone)]
pub enum Response {
    Prepare { seq: usize, value: Option<u32> },
    Accept { seq: usize },
}

impl Response {

    fn len(&self) -> usize {
        let n_usize = std::mem::size_of::<usize>();
        let n_u32 = std::mem::size_of::<u32>();
        match self {
            Response::Prepare{ seq: _, value: Some(_) } => {
                1 + n_usize + 1 + n_u32
            }
            Response::Prepare{ seq: _, value: None } => {
                1 + n_usize + 1
            }
            Response::Accept{ seq: _ } => 1 + n_usize,
        }
    }
    
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Response::Prepare{ seq, value } => {
                buf.put_u8(0);
                buf.put_uint_be(*seq as u64, std::mem::size_of::<usize>());
                match value {
                    Some(v) => {
                        buf.put_u8(1);
                        buf.put_u32_be(*v);
                    }
                    None => {
                        buf.put_u8(0);
                    }
                }
            }
            Response::Accept{ seq } => {
                buf.put_u8(1);
                buf.put_uint_be(*seq as u64, std::mem::size_of::<usize>());
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Response {
        let ty = buf.split_to(1)[0];
        match ty {
            0 => {
                let seq = get_usize_from_buf(buf);
                match buf.split_to(1)[0] {
                    0 => {
                        Response::Prepare{ seq, value: None }
                    }
                    1 => {
                        let value = get_u32_from_buf(buf);
                        Response::Prepare{ seq, value: Some(value) }
                    }
                    _ => {
                        panic!("found corrupt data while decoding to Request")
                    }
                }
            }
            1 => {
                let seq = get_usize_from_buf(buf);
                Response::Accept{ seq }
            }
            _ => {
                panic!("found corrupt data while decoding to Request")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Datagram {
    Request(Request),
    Response(Response),
}

impl Datagram {
    fn len(&self) -> usize {
        
        match self {
            Datagram::Request(req) => 1 + req.len(),
            Datagram::Response(resp) => 1 + resp.len(),
        }

    }

    pub fn encode(&self, buf: &mut BytesMut) {
        const N_USIZE: usize = std::mem::size_of::<usize>();
        let n = self.len();
        buf.reserve(N_USIZE + n);
        buf.put_uint_be(n as u64, N_USIZE);
        match self {
            Datagram::Request(req) => {
                buf.put_u8(0);
                req.encode(buf);
            }
            Datagram::Response(resp) => {
                buf.put_u8(1);
                resp.encode(buf);
            }
        }
    }

    pub fn decode(buf: &mut BytesMut) -> Option<Datagram> {
        // dbg!("decode");
        const N_USIZE: usize = std::mem::size_of::<usize>();
        // dbg!(buf.len());
        if buf.len() < N_USIZE {
            None
        }
        else {
            let mut space = [0; N_USIZE];
            space.clone_from_slice(&buf[..N_USIZE]);
            let n = usize::from_be_bytes(space);
            // dbg!(n);
            if buf.len() >= N_USIZE + n {
                buf.advance(N_USIZE);
                let ty = buf.split_to(1)[0];
                Some(match ty {
                    0 => Datagram::Request(Request::decode(buf)),
                    1 => Datagram::Response(Response::decode(buf)),
                    _ => panic!("found corrupt data while decoding to Request"),
                })
            }
            else {
                None
            }
        }
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
    pub fn new(peers_id: HashSet<usize>, tx: Tx<Outgoing>, rx: Rx<Incoming>) -> Self {
        println!("Paxos start with peers_num: {:?}", peers_id);
        Paxos {
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

    fn next_seq(&mut self) -> usize {
        self.current_seq += 1;
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
        println!("handle request: {:?}", req);
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
                println!("value learned {}", self.chosen.unwrap());
            }
            Request::Propose{value} => {
                if let Some(ref _proposal) = self.proposal {
                    println!("override an existed proposal.");
                }
                let seq = self.next_seq();
                self.proposal = Some(Proposal{
                    seq: seq,
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
        println!("handle resp: {:?}", resp);
        match resp {
            Response::Prepare{seq, value} => {
                if let Some(ref mut proposal) = self.proposal {
                    assert!(proposal.seq >= seq);
                    proposal.prepared.insert(src);
                    if proposal.prepared.len() <= self.peers_id.len() / 2
                        && seq >= *proposal.highest_seq.get_or_insert(seq)
                    {
                        proposal.value = value;
                    }
                    if proposal.prepared.len() == 1 + self.peers_id.len() / 2 {
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
                println!("handle accept resp seq: {}", seq);
                if let Some(ref mut proposal) = self.proposal {
                    if seq == proposal.seq {
                        proposal.accepted.insert(src);
                        if proposal.accepted.len() == 1 + self.peers_id.len() / 2 {
                            assert!(proposal.value.is_some());
                            self.chosen = proposal.value;
                            if self.chosen.unwrap() == proposal.wanted_value {
                                println!("proposal value `{}` success.", self.chosen.unwrap());
                            }
                            else {
                                println!("proposal value `{}` fail, `{}` is chosen.",
                                    proposal.wanted_value, self.chosen.unwrap());
                            }
                            let req = Request::Learn{
                                value: self.chosen.unwrap()
                            };
                            println!("value accepted by majority: {}", self.chosen.unwrap());
                            self.tx.unbounded_send(Outgoing{
                                dst: proposal.accepted.clone(),
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


// #[derive(Debug)]
// struct PaxosServer {
//     paxos: Paxos,
//     broker: Broker,
// }

impl Future for Paxos {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.rx.poll()? {
                Async::Ready(Some(incoming)) => self.handle_incoming(incoming),
                Async::Ready(None) => return Ok(Async::Ready(())),
                Async::NotReady => return Ok(Async::NotReady),
            }
        }
    }
}
