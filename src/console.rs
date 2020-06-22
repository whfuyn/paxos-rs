use futures::channel::mpsc;
use std::collections::HashMap;
use std::io::BufRead;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::prelude::*;

use crate::network::*;
use crate::paxos::*;

macro_rules! print_flushed {
    ($($tokens: tt)*) => {
        {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            write!(handle, $($tokens)*).unwrap();
            handle.flush().unwrap();
        }
    }
}

macro_rules! println_flushed {
    ($($tokens: tt)*) => {
        {
            use std::io::Write;
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            writeln!(handle, $($tokens)*).unwrap();
            handle.flush().unwrap();
        }
    }
}

#[derive(Debug, PartialEq)]
enum Command {
    Start(usize),
    Propose(usize, u32),
    Query(usize),
    Exit,
}

#[derive(Debug, PartialEq)]
struct ParseCommandError;

impl From<std::option::NoneError> for ParseCommandError {
    fn from(_: std::option::NoneError) -> Self {
        ParseCommandError
    }
}

impl FromStr for Command {
    type Err = ParseCommandError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        let mut tokens = lower.split_whitespace();
        match tokens.next()? {
            "s" | "start" => {
                let num = tokens.next()?.parse::<usize>().unwrap();
                Ok(Self::Start(num))
            }
            "p" | "propose" => {
                let id = tokens.next()?.parse::<usize>().unwrap();
                let val = tokens.next()?.parse::<u32>().unwrap();
                Ok(Self::Propose(id, val))
            }
            "q" | "query" => {
                let id = tokens.next()?.parse::<usize>().unwrap();
                Ok(Self::Query(id))
            }
            "x" | "exit" => Ok(Self::Exit),
            _ => Err(ParseCommandError),
        }
    }
}

pub struct Console {
    rt: tokio::runtime::Runtime,
    addr_table: Option<Arc<HashMap<usize, SocketAddr>>>,
}

impl Console {
    pub fn new() -> Self {
        Self {
            rt: tokio::runtime::Runtime::new().unwrap(),
            addr_table: None,
        }
    }

    pub fn run(mut self) {
        let stdin = std::io::stdin();
        let handle = stdin.lock();
        print_flushed!("Paxos> ");
        for line in handle.lines() {
            if let Ok(line) = line {
                if let Ok(cmd) = line.parse::<Command>() {
                    match cmd {
                        Command::Start(num) => self.start_servers(num, 12345),
                        Command::Propose(server_id, val) => self.propose(server_id, val),
                        Command::Query(server_id) => self.query(server_id),
                        Command::Exit => break,
                    }
                } else {
                    println_flushed!("Unknown command.");
                }
            }
            // A slight pause waiting for servers' output.
            // Otherwise the prompt will mess up with them.
            std::thread::sleep(std::time::Duration::from_millis(200));
            print_flushed!("Paxos> ");
        }
    }

    fn query(&mut self, server_id: usize) {
        if let Some(peers_addr) = &self.addr_table {
            if let Some(addr) = peers_addr.get(&server_id) {
                let addr = addr.clone();
                let task = async move {
                    if let Ok(mut stream) = TcpStream::connect(addr).await {
                        let dgram = Datagram::Request(Request::Query);
                        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
                    }
                };
                self.rt.block_on(task);
            } else {
                println_flushed!("error: server id dosen't exist.");
            }
        } else {
            println_flushed!("error: servers haven't started.");
        }
    }

    fn propose(&mut self, server_id: usize, val: u32) {
        if let Some(addr_table) = &self.addr_table {
            if let Some(addr) = addr_table.get(&server_id) {
                let addr = addr.clone();
                let task = async move {
                    if let Ok(mut stream) = TcpStream::connect(addr).await {
                        let dgram = Datagram::Request(Request::Propose { value: val });
                        stream.write_all(&dgram.encode_with_src(0)).await.unwrap();
                    }
                };
                self.rt.block_on(task);
            } else {
                println_flushed!("error: server id dosen't exist.");
            }
        } else {
            println_flushed!("error: servers haven't started.");
        }
    }

    fn start_servers(&mut self, server_num: usize, base_port: usize) {
        let server_num = server_num + 1; // #0 for client.
        let addr_table: Arc<HashMap<usize, SocketAddr>> = Arc::new(
            ((base_port)..(base_port + server_num))
                .enumerate()
                .map(|(id, port)| (id, format!("127.0.0.1:{}", port).parse().unwrap()))
                .collect(),
        );
        let start_server = |id: usize| {
            let (itx, irx) = mpsc::unbounded();
            let (otx, orx) = mpsc::unbounded();
            // skip client #0
            let paxos = Paxos::new(id, (1..server_num).collect(), otx, irx);
            let broker = Broker::new(id, (*addr_table).clone());
            self.rt.spawn(broker.run(itx, orx));
            self.rt.spawn(paxos.run());
        };
        (0..server_num).for_each(|id| {
            start_server(id);
        });
        self.addr_table = Some(addr_table.clone());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_command_parse() {
        let start = "start 4";
        assert_eq!(start.parse::<Command>(), Ok(Command::Start(4)));
        let propose = "propose 0 42";
        assert_eq!(propose.parse::<Command>(), Ok(Command::Propose(0, 42)));
        let exit = "exit";
        assert_eq!(exit.parse::<Command>(), Ok(Command::Exit));
        let error = "error";
        assert_eq!(error.parse::<Command>(), Err(ParseCommandError));
    }
}
