use crate::*;
use derive_new::new;
use std::time::Instant;

pub trait MsgRequest {
    type Response: Send + 'static;
    type Ack: Send + 'static;

    fn ack(&self) -> Option<Self::Ack> {
        None
    }
    fn response(&self, _success: bool) -> Option<Self::Response> {
        None
    }
}

pub trait TxnRequest: MsgRequest {
    fn id(&self) -> TxnId;
    fn start_ts(&self) -> Ts;
    fn timeout(&self) -> Instant;
}

#[derive(Debug, Clone, new)]
pub struct LockRequest {
    pub key: Key,
    pub id: TxnId,
    pub start_ts: Ts,
    pub current_time: Instant,
    pub timeout: Instant,
    pub for_update_ts: Ts,
}

impl MsgRequest for LockRequest {
    type Response = LockResponse;
    type Ack = LockAck;

    fn ack(&self) -> Option<LockAck> {
        Some(LockAck {
            key: self.key,
            id: self.id,
        })
    }

    fn response(&self, success: bool) -> Option<LockResponse> {
        Some(LockResponse {
            key: self.key,
            id: self.id,
            success,
        })
    }
}

impl TxnRequest for LockRequest {
    fn id(&self) -> TxnId {
        self.id
    }

    fn start_ts(&self) -> Ts {
        self.start_ts
    }

    fn timeout(&self) -> Instant {
        self.timeout
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

#[derive(Debug, Clone, new)]
pub struct PrewriteRequest {
    pub id: TxnId,
    pub start_ts: Ts,
    pub commit_ts: Ts,
    pub current_time: Instant,
    pub timeout: Instant,
    pub writes: Vec<(Key, Value)>,
}

impl MsgRequest for PrewriteRequest {
    type Response = PrewriteResponse;
    type Ack = PrewriteAck;

    fn ack(&self) -> Option<PrewriteAck> {
        Some(PrewriteAck { id: self.id })
    }

    fn response(&self, success: bool) -> Option<PrewriteResponse> {
        Some(PrewriteResponse {
            id: self.id,
            success,
        })
    }
}

impl TxnRequest for PrewriteRequest {
    fn id(&self) -> TxnId {
        self.id
    }

    fn start_ts(&self) -> Ts {
        self.start_ts
    }

    fn timeout(&self) -> Instant {
        self.timeout
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

#[derive(Debug, Clone, new)]
pub struct FinaliseRequest {
    pub id: TxnId,
}

impl MsgRequest for FinaliseRequest {
    type Response = !;
    type Ack = UnitAck;

    fn ack(&self) -> Option<UnitAck> {
        Some(UnitAck { id: self.id })
    }
}

#[derive(Debug, Clone, new)]
pub struct RollbackRequest {
    pub id: TxnId,
    pub keys: Vec<Key>,
}

impl MsgRequest for RollbackRequest {
    type Response = !;
    type Ack = UnitAck;

    fn ack(&self) -> Option<UnitAck> {
        Some(UnitAck { id: self.id })
    }
}

#[derive(Debug, Clone)]
pub struct Shutdown;

impl MsgRequest for Shutdown {
    type Response = !;
    type Ack = !;
}

#[derive(Debug, Clone)]
pub struct UnitAck {
    pub id: TxnId,
}
