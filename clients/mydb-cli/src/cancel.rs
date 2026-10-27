use mysql::{prelude::Queryable, Conn, Opts};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc, Arc, Condvar, Mutex,
};

#[derive(Default)]
struct State {
    generation: u64,
    connection: Option<u32>,
    finished: bool,
    cancelling: bool,
    interrupted: bool,
}

pub struct Cancellation {
    shared: Arc<(Mutex<State>, Condvar)>,
    hits: Arc<AtomicUsize>,
}

impl Cancellation {
    pub fn new(opts: Opts) -> Result<Self, String> {
        let shared = Arc::new((Mutex::new(State::default()), Condvar::new()));
        let hits = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = mpsc::channel::<(u64, u32)>();
        let state = shared.clone();
        let count = hits.clone();
        ctrlc::set_handler(move || {
            // The second interrupt must not wait for a blocking control query.
            if count.fetch_add(1, Ordering::SeqCst) != 0 {
                std::process::exit(130);
            }
            let mut lock = state.0.lock().unwrap_or_else(|e| e.into_inner());
            let Some(id) = lock.connection else {
                std::process::exit(130);
            };
            lock.interrupted = true;
            lock.cancelling = true;
            let _ = sender.send((lock.generation, id));
        })
        .map_err(|e| e.to_string())?;
        let state = shared.clone();
        std::thread::Builder::new()
            .name("mydb-cli-cancel".into())
            .spawn(move || {
                while let Ok((generation, id)) = receiver.recv() {
                    let control = Conn::new(opts.clone());
                    let mut lock = state.0.lock().unwrap_or_else(|e| e.into_inner());
                    if lock.generation == generation {
                        if !lock.finished {
                            // Keep the generation locked through KILL. The main connection
                            // cannot begin its next statement before the acknowledgement.
                            let result =
                                control.and_then(|mut c| c.query_drop(format!("KILL QUERY {id}")));
                            if let Err(e) = result {
                                eprintln!("mydb-cli: could not cancel query: {e}; press Ctrl+C again to exit");
                            }
                        }
                        lock.cancelling = false;
                        state.1.notify_all();
                    }
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Self { shared, hits })
    }
    pub fn begin(&self, id: u32) {
        let mut s = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        s.generation += 1;
        s.connection = Some(id);
        s.finished = false;
        s.interrupted = false;
        s.cancelling = false;
        self.hits.store(0, Ordering::SeqCst);
    }
    pub fn interrupted(&self) -> bool {
        self.hits.load(Ordering::Relaxed) != 0
    }
    pub fn finish(&self) -> bool {
        let mut s = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        s.finished = true;
        while s.cancelling {
            s = self.shared.1.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        s.connection = None;
        s.interrupted
    }
}
