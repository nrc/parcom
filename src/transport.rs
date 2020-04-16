use crate::messages::Shutdown;
use std::{
    any,
    sync::{mpsc, Arc, Condvar, Mutex},
    thread,
};

pub fn new<T: Receiver>() -> (TransportSend, TransportRecv<T>) {
    let (send, recv) = mpsc::channel();
    let queue = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let resender = Resender {
        queue: queue.clone(),
        channel: send,
    };
    thread::spawn(move || resender.start());
    (
        TransportSend { queue },
        TransportRecv {
            channel: recv,
            recv: None,
        },
    )
}

#[derive(Debug, Clone)]
pub struct TransportSend {
    queue: Arc<(Mutex<Vec<Box<dyn any::Any + Send>>>, Condvar)>,
}

pub struct TransportRecv<T: Receiver> {
    channel: mpsc::Receiver<Box<dyn any::Any + Send>>,
    recv: Option<Arc<T>>,
}

#[derive(Clone)]
struct Resender {
    channel: mpsc::Sender<Box<dyn any::Any + Send>>,
    queue: Arc<(Mutex<Vec<Box<dyn any::Any + Send>>>, Condvar)>,
}

impl TransportSend {
    pub fn send(&self, msg: Box<dyn any::Any + Send>) {
        let mut queue = self.queue.0.lock().unwrap();
        queue.push(msg);
        self.queue.1.notify_one();
    }

    pub fn shutdown(&self) {
        let mut queue = self.queue.0.lock().unwrap();
        queue.push(Box::new(Shutdown));
        self.queue.1.notify_one();
    }
}

impl Resender {
    fn start(&self) {
        loop {
            let mut queue = self.queue.0.lock().unwrap();

            for msg in queue.drain(..) {
                self.channel
                    .send(msg)
                    .map_err(|e| format!("Sending message failed: {}", e))
                    .unwrap();
            }

            let _ = self.queue.1.wait(queue).unwrap();
        }
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
