use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

use apm::event::{CommandStarted, CommandResult};
use Client;
use error::Result;

pub type StartHook = fn(Client, &CommandStarted);
pub type CompletionHook = fn(Client, &CommandResult);

pub struct Listener {
    no_start_hooks: AtomicBool,
    no_completion_hooks: AtomicBool,
    start_hooks: RwLock<Vec<StartHook>>,
    completion_hooks: RwLock<Vec<CompletionHook>>,
}

impl Listener {
    pub fn new() -> Listener {
        Listener {
            no_start_hooks: AtomicBool::new(true),
            no_completion_hooks: AtomicBool::new(true),
            start_hooks: RwLock::new(Vec::new()),
            completion_hooks: RwLock::new(Vec::new()),
        }
    }

    pub fn add_start_hook(&self, hook: StartHook) -> Result<()> {
        let mut guard = self.start_hooks.write()?;
        self.no_start_hooks.store(false, Ordering::SeqCst);
        Ok(guard.deref_mut().push(hook))
    }

    pub fn add_completion_hook(&self, hook: CompletionHook) -> Result<()> {
        let mut guard = self.completion_hooks.write()?;
        self.no_completion_hooks.store(false, Ordering::SeqCst);
        Ok(guard.deref_mut().push(hook))
    }

    pub fn run_start_hooks(&self, client: Client, started: &CommandStarted) -> Result<()> {
        if self.no_start_hooks.load(Ordering::SeqCst) {
            return Ok(());
        }

        let guard = self.start_hooks.read()?;

        for hook in guard.deref().iter() {
            hook(client.clone(), started);
        }

        Ok(())
    }

    pub fn run_completion_hooks(&self, client: Client, result: &CommandResult) -> Result<()> {
        if self.no_completion_hooks.load(Ordering::SeqCst) {
            return Ok(());
        }

        let guard = self.completion_hooks.read()?;

        for hook in guard.deref().iter() {
            hook(client.clone(), result);
        }

        Ok(())
    }
}
