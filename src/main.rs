#![feature(try_trait)]

use console::Console;

mod network;
mod paxos;
mod console;


fn main() {
    let console = Console::new();
    console.run();
}