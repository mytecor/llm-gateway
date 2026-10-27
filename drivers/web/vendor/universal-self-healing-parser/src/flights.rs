//! Process-local coordination, shared by parsers using the same store allocation.
use crate::{Error, Result, RuleStore};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::Poll,
};
use tokio::sync::watch;

type Outcome = std::result::Result<(), String>;
type Key = (u64, String);
type Receiver = watch::Receiver<Option<Outcome>>;
type Subscription = (String, Receiver);

#[derive(Default)]
pub(crate) struct Flights {
    active: Mutex<BTreeMap<Key, watch::Sender<Option<Outcome>>>>,
}

pub(crate) enum Claim {
    Refresh,
    Wait(Vec<Subscription>),
    Lead(Leader),
}

impl Flights {
    pub fn for_store(store: &Arc<dyn RuleStore>) -> Arc<Self> {
        static STORES: OnceLock<Mutex<BTreeMap<usize, Weak<Flights>>>> = OnceLock::new();
        let key = Arc::as_ptr(store) as *const () as usize;
        let mut stores = STORES.get_or_init(Mutex::default).lock().unwrap();
        stores.retain(|_, flights| flights.strong_count() > 0);
        if let Some(flights) = stores.get(&key).and_then(Weak::upgrade) {
            return flights;
        }
        let flights = Arc::new(Self::default());
        stores.insert(key, Arc::downgrade(&flights));
        flights
    }

    /// Reserve a whole batch atomically. Never hold some rules while waiting for others.
    pub fn claim(
        self: &Arc<Self>,
        store: &dyn RuleStore,
        revision: u64,
        ids: &[String],
    ) -> Result<Claim> {
        let mut active = self.active.lock().map_err(|_| Error::StorePoisoned)?;
        // Close the gap between reading a snapshot and registering its healing attempt.
        if store.snapshot()?.revision() != revision {
            return Ok(Claim::Refresh);
        }
        let waiting: Vec<_> = ids
            .iter()
            .filter_map(|id| {
                active
                    .get(&(revision, id.clone()))
                    .map(|tx| (id.clone(), tx.subscribe()))
            })
            .collect();
        if !waiting.is_empty() {
            return Ok(Claim::Wait(waiting));
        }
        let keys: Vec<_> = ids.iter().map(|id| (revision, id.clone())).collect();
        for key in &keys {
            active.insert(key.clone(), watch::channel(None).0);
        }
        Ok(Claim::Lead(Leader {
            flights: self.clone(),
            keys,
        }))
    }
}

async fn wait(mut receiver: Receiver) -> Result<()> {
    loop {
        if let Some(outcome) = receiver.borrow_and_update().clone() {
            return outcome.map_err(Error::Healing);
        }
        receiver
            .changed()
            .await
            .map_err(|_| Error::Healing("healing cancelled".into()))?;
    }
}

pub(crate) struct Leader {
    flights: Arc<Flights>,
    keys: Vec<Key>,
}
impl Leader {
    pub fn finish(mut self, outcome: Outcome) {
        self.publish(outcome);
    }
    fn publish(&mut self, outcome: Outcome) {
        let mut active = self
            .flights
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for key in self.keys.drain(..) {
            if let Some(sender) = active.remove(&key) {
                sender.send_replace(Some(outcome.clone()));
            }
        }
    }
}
impl Drop for Leader {
    fn drop(&mut self) {
        self.publish(Err("healing cancelled".into()));
    }
}

/// Poll all subscriptions so a failure is observed even while another flight is pending.
pub(crate) async fn wait_all(receivers: Vec<Receiver>) -> Result<()> {
    let mut pending: Vec<_> = receivers.into_iter().map(|rx| Box::pin(wait(rx))).collect();
    std::future::poll_fn(move |cx| {
        let mut index = 0;
        while index < pending.len() {
            match pending[index].as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    drop(pending.swap_remove(index));
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => index += 1,
            }
        }
        if pending.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
    .await
}
