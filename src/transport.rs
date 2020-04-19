use crate::messages::Shutdown;
use std::{
    any,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
};

pub fn new<T: Receiver>() -> (TransportSend, TransportRecv<T>, thread::JoinHandle<()>) {
    let (send, recv) = mpsc::channel();
    let queue = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let resender = Resender {
        queue: queue.clone(),
        channel: send,
    };
    let handle = thread::spawn(move || resender.start());
    (
        TransportSend {
            queue,
            shutdown: AtomicBool::new(false),
        },
        TransportRecv {
            channel: recv,
            recv: None,
        },
        handle,
    )
}

#[derive(Debug)]
pub struct TransportSend {
    queue: Arc<(Mutex<Vec<Box<dyn any::Any + Send>>>, Condvar)>,
    shutdown: AtomicBool,
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
        assert!(!self.shutdown.load(Ordering::SeqCst));
        let mut queue = self.queue.0.lock().unwrap();
        queue.push(msg);
        self.queue.1.notify_one();
    }

    pub fn shutdown(&self) {
        assert!(!self.shutdown.load(Ordering::SeqCst));
        let mut queue = self.queue.0.lock().unwrap();
        queue.push(Box::new(Shutdown));
        self.shutdown.store(true, Ordering::SeqCst);
        self.queue.1.notify_one();
    }
}

impl Resender {
    fn start(&self) {
        loop {
            let mut queue = self.queue.0.lock().unwrap();

            for msg in queue.drain(..) {
                let shutdown = msg.is::<Shutdown>();
                self.channel
                    .send(msg)
                    .map_err(|e| format!("Sending message failed: {}", e))
                    .unwrap();
                if shutdown {
                    return;
                }
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
                    self.recv.clone().unwrap().handle_shutdown().unwrap();
                    eprintln!("listener shutting down normally");
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
