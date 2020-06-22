#![feature(try_trait)]

use console::Console;

mod console;
mod network;
mod paxos;

fn main() {
    let console = Console::new();
    console.run();
}
