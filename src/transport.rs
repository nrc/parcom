use crate::*;
use std::{
    any,
    sync::{mpsc, Arc},
    thread,
};

pub fn new<T: Receiver>() -> (TransportSend, TransportRecv<T>) {
    let (send, recv) = mpsc::channel();
    (
        TransportSend { channel: send },
        TransportRecv {
            channel: recv,
            recv: None,
        },
    )
}

#[derive(Debug, Clone)]
pub struct TransportSend {
    channel: mpsc::Sender<Box<dyn any::Any + Send>>,
}

pub struct TransportRecv<T: Receiver> {
    channel: mpsc::Receiver<Box<dyn any::Any + Send>>,
    recv: Option<Arc<T>>,
}

impl TransportSend {
    pub fn send(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        self.channel
            .send(msg)
            .map_err(|e| format!("Sending message failed: {}", e))
    }
}

impl<T: Receiver + 'static> TransportRecv<T> {
    pub fn set_handler(&mut self, t: Arc<T>) {
        self.recv = Some(t);
    }

    pub fn listen(self) -> thread::JoinHandle<()> {
        thread::spawn(move || loop {
            if let Err(e) = self
                .channel
                .recv()
                .map_err(|e| e.to_string())
                .and_then(|msg| self.recv.as_ref().unwrap().recv_msg(msg))
            {
                eprintln!("listener shutting down: {}", e);
                return;
            }
        })
    }
}

pub trait Receiver: Send + Sync {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct LockRequest {
    pub key: Key,
    pub start_ts: Ts,
    pub for_update_ts: Ts,
}

impl LockRequest {
    pub fn ack(&self) -> LockAck {
        LockAck {
            key: self.key,
            start_ts: self.start_ts,
        }
    }

    pub fn response(&self, success: bool) -> LockResponse {
        LockResponse {
            key: self.key,
            start_ts: self.start_ts,
            success,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockAck {
    pub key: Key,
    pub start_ts: Ts,
}

#[derive(Debug, Clone)]
pub struct LockResponse {
    pub key: Key,
    pub start_ts: Ts,
    // FIXME needs more detail if we want to retry
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct PrewriteRequest {
    pub start_ts: Ts,
    pub commit_ts: Ts,
    pub writes: Vec<(Key, Value)>,
}

#[derive(Debug, Clone)]
pub struct PrewriteAck {
    pub start_ts: Ts,
}

#[derive(Debug, Clone)]
pub struct PrewriteResponse {
    pub start_ts: Ts,
    // FIXME needs more detail if we want to retry
    pub success: bool,
}
