use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(true);

pub fn init(level: &str) {
    ENABLED.store(!level.eq_ignore_ascii_case("off"), Ordering::Relaxed);
}

pub fn info(message: &str) {
    if ENABLED.load(Ordering::Relaxed) {
        println!("[INFO] {message}");
    }
}
