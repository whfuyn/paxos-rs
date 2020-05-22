# paxos-rs
[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

An implementation of paxos algorithm in rust, based on the paper "Paxos Made Simple".

This project is for learning purpose and praticing my rust programming. Its goal is to implement the paxos algorithm
as is discussed in "Paxos Made Simple", in which every server can start a proposal and finally all servers reach consensus
in a chosen value.

# Usage

Run this program in four different consoles with 1, 2, 3, 0 respectively as its parameter which denotes the server's id. 
(0 must be the last one to run since the 0th server will initiate the first request.)

Before the 0th server begin, you will see something like 

![alt text](before.jpg)

After the first proposal started, all the four servers learned the chosen value 42.

leader:
![alt text](server0.jpg)

follower:
![alt text](server1.jpg)
