use crate::*;

pub trait MsgRequest {
    type Response: Send + 'static;
    type Ack: Send + 'static;

    fn ack(&self) -> Self::Ack;
    fn response(&self, success: bool) -> Self::Response;
}

#[derive(Debug, Clone)]
pub struct LockRequest {
    pub key: Key,
    pub start_ts: Ts,
    pub for_update_ts: Ts,
}

impl MsgRequest for LockRequest {
    type Response = LockResponse;
    type Ack = LockAck;

    fn ack(&self) -> LockAck {
        LockAck {
            key: self.key,
            start_ts: self.start_ts,
        }
    }

    fn response(&self, success: bool) -> LockResponse {
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

impl MsgRequest for PrewriteRequest {
    type Response = PrewriteResponse;
    type Ack = PrewriteAck;

    fn ack(&self) -> PrewriteAck {
        PrewriteAck {
            start_ts: self.start_ts,
        }
    }

    fn response(&self, success: bool) -> PrewriteResponse {
        PrewriteResponse {
            start_ts: self.start_ts,
            success,
        }
    }
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

#[derive(Debug, Clone)]
pub struct FinaliseRequest {
    pub start_ts: Ts,
}

impl MsgRequest for FinaliseRequest {
    type Response = !;
    type Ack = !;

    fn ack(&self) -> ! {
        panic!();
    }

    fn response(&self, _: bool) -> ! {
        panic!();
    }
}

#[derive(Debug, Clone)]
pub struct RollbackRequest {
    pub start_ts: Ts,
    pub keys: Vec<Key>,
}

impl MsgRequest for RollbackRequest {
    type Response = !;
    type Ack = !;

    fn ack(&self) -> ! {
        panic!();
    }

    fn response(&self, _: bool) -> ! {
        panic!();
    }
}

#[derive(Debug, Clone)]
pub struct Shutdown;

impl MsgRequest for Shutdown {
    type Response = !;
    type Ack = !;

    fn ack(&self) -> ! {
        panic!();
    }

    fn response(&self, _: bool) -> ! {
        panic!();
    }
}
