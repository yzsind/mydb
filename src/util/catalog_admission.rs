use std::sync::Arc;

pub type CatalogReadPermit = parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>;

/// Keep synchronous catalog readers off a reactor when a logical writer owns
/// the catalog. Writers queue here before entering the live-catalog latch, so
/// recursive reads within an admitted attempt cannot encounter a queued writer.
#[derive(Debug, Default)]
pub struct CatalogAdmission {
    gate: Arc<parking_lot::RwLock<()>>,
    changed: tokio::sync::Notify,
}

impl CatalogAdmission {
    pub fn try_read(&self) -> Option<CatalogReadPermit> {
        self.gate.try_read_arc()
    }

    pub async fn read(&self) -> CatalogReadPermit {
        if let Some(permit) = self.try_read() {
            return permit;
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(permit) = self.try_read() {
                return permit;
            }
            changed.await;
        }
    }

    pub(crate) fn write(&self) -> CatalogWritePermit<'_> {
        CatalogWritePermit {
            guard: Some(self.gate.write()),
            admission: self,
        }
    }
}

pub(crate) struct CatalogWritePermit<'a> {
    guard: Option<parking_lot::RwLockWriteGuard<'a, ()>>,
    admission: &'a CatalogAdmission,
}

impl Drop for CatalogWritePermit<'_> {
    fn drop(&mut self) {
        self.guard.take();
        self.admission.changed.notify_waiters();
    }
}
