use crate::*;
use std::{
    any,
    sync::{mpsc, Arc},
    thread,
};

pub fn new<T: Receiver>(t: Arc<T>) -> (TransportSend, TransportRecv<T>) {
    let (send, recv) = mpsc::channel();
    (
        TransportSend { channel: send },
        TransportRecv {
            channel: recv,
            recv: t,
        },
    )
}

#[derive(Debug, Clone)]
pub struct TransportSend {
    channel: mpsc::Sender<Box<dyn any::Any + Send>>,
}

pub struct TransportRecv<T: Receiver> {
    channel: mpsc::Receiver<Box<dyn any::Any + Send>>,
    recv: Arc<T>,
}

impl TransportSend {
    pub fn send(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        self.channel
            .send(msg)
            .map_err(|e| format!("Sending message failed: {}", e))
    }
}

impl<T: Receiver + 'static> TransportRecv<T> {
    pub fn listen(self) {
        thread::spawn(move || loop {
            if let Err(_) = self
                .channel
                .recv()
                .map_err(|e| e.to_string())
                .and_then(|msg| self.recv.recv_msg(msg))
            {
                return;
            }
        });
    }
}

pub trait Receiver: Send + Sync {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct LockMsg {
    pub key: Key,
    pub start_ts: Ts,
    pub for_update_ts: Ts,
}

#[derive(Debug, Clone)]
pub struct PrewriteMsg {
    pub start_ts: Ts,
    pub commit_ts: Ts,
    pub writes: Vec<(Key, Value)>,
}
