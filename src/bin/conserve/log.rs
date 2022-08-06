use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::ops::Deref;

use lazy_static::lazy_static;
use tracing::metadata::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;
use tracing_subscriber::fmt;

struct TerminalWriter { }

impl TerminalWriter { }

lazy_static!{
    pub static ref TERMINAL_OUTPUT: Mutex<Option<Arc<Mutex<dyn Write + Send + Sync>>>> = Mutex::new(
        Some(Arc::new(Mutex::new(std::io::stdout())))        
    );
}

impl Write for TerminalWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let current_target = TERMINAL_OUTPUT.lock().expect("lock() should not fail");
        if let Some(target) = current_target.deref() {
            let mut target = target.lock().expect("lock() should not fail");
            target.write(buf)
        } else {
            Ok(buf.len())
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let output = TERMINAL_OUTPUT.lock().expect("lock() should not fail");
        if let Some(target) = output.deref() {
            let mut target = target.lock().expect("lock() should not fail");
            target.flush()
        } else {
            Ok(())
        }
    }
}

pub struct LoggingOptions {
    pub file: Option<PathBuf>,
    pub level: tracing::Level,
}

pub fn init(options: LoggingOptions) -> std::result::Result<LogGuard, String> {
    let subscriber = Registry::default()
        .with(
            fmt::Layer::default()
                .with_target(false)
                .with_writer(|| TerminalWriter{})
                .with_filter(LevelFilter::from(options.level))
        );

    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| "Failed to update global default logger".to_string())?;

    Ok(LogGuard{ })
}

/// Guards all logging activity.
/// When dropping the pending logs will be written synchronously
/// and all open handles closed.
pub struct LogGuard {

}

pub struct ViewLogGuard {
    released: bool,
    previous_logger: Option<Arc<Mutex<dyn Write + Send + Sync>>>,
}

impl ViewLogGuard {
    fn restore_previous_(&mut self) {
        if self.released {
            return;
        }

        self.released = true;
        
        let mut output = TERMINAL_OUTPUT.lock().unwrap();
        *output = self.previous_logger.take();
    }
}

impl Drop for ViewLogGuard {
    fn drop(&mut self) {
        self.restore_previous_();
    }
}

pub fn update_terminal_target(target: Arc<Mutex<dyn Write + Send + Sync>>) -> ViewLogGuard {
    let mut output = TERMINAL_OUTPUT.lock().unwrap();
    let previous_logger = output.replace(target);

    ViewLogGuard { previous_logger, released: false }
}