use crate::messages::Shutdown;
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
        eprintln!("sending");
        let result = self
            .channel
            .send(msg)
            .map_err(|e| format!("Sending message failed: {}", e));
        eprintln!("sent");
        result
    }

    pub fn shutdown(&self) {
        self.channel
            .send(Box::new(Shutdown))
            .map_err(|e| format!("Sending shutdown message failed: {}", e))
            .unwrap();
    }
}

impl<T: Receiver + 'static> TransportRecv<T> {
    pub fn set_handler(&mut self, t: Arc<T>) {
        self.recv = Some(t);
    }

    pub fn listen(self) -> thread::JoinHandle<()> {
        thread::spawn(move || loop {
            let msg = self.channel.recv().map_err(|e| e.to_string());
            if let Ok(msg) = &msg {
                if msg.is::<Shutdown>() {
                    eprintln!("listener shutting down normally");
                    self.recv.clone().unwrap().handle_shutdown().unwrap();
                    return;
                }
            }
            if let Err(e) = msg.and_then(|msg| self.recv.clone().unwrap().recv_msg(msg)) {
                eprintln!("listener shutting down: {}", e);
                return;
            }
        })
    }
}

pub trait Receiver: Send + Sync {
    fn recv_msg(self: Arc<Self>, msg: Box<dyn any::Any + Send>) -> Result<(), String>;
    fn handle_shutdown(self: Arc<Self>) -> Result<(), String>;
}
