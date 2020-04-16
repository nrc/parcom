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
    pub id: TxnId,
    pub start_ts: Ts,
    pub for_update_ts: Ts,
}

impl MsgRequest for LockRequest {
    type Response = LockResponse;
    type Ack = LockAck;

    fn ack(&self) -> LockAck {
        LockAck {
            key: self.key,
            id: self.id,
        }
    }

    fn response(&self, success: bool) -> LockResponse {
        LockResponse {
            key: self.key,
            id: self.id,
            success,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockAck {
    pub id: TxnId,
    pub key: Key,
}

#[derive(Debug, Clone)]
pub struct LockResponse {
    pub id: TxnId,
    pub key: Key,
    // FIXME needs more detail if we want to retry
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct PrewriteRequest {
    pub id: TxnId,
    pub start_ts: Ts,
    pub commit_ts: Ts,
    pub writes: Vec<(Key, Value)>,
}

impl MsgRequest for PrewriteRequest {
    type Response = PrewriteResponse;
    type Ack = PrewriteAck;

    fn ack(&self) -> PrewriteAck {
        PrewriteAck { id: self.id }
    }

    fn response(&self, success: bool) -> PrewriteResponse {
        PrewriteResponse {
            id: self.id,
            success,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrewriteAck {
    pub id: TxnId,
}

#[derive(Debug, Clone)]
pub struct PrewriteResponse {
    pub id: TxnId,
    // FIXME needs more detail if we want to retry
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct FinaliseRequest {
    pub id: TxnId,
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
    pub id: TxnId,
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
