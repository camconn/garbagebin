//! The clock Object Lock judges retention by: system time, or a fixed instant
//! (`--now`) that tests can move.

use std::sync::Mutex;
use std::time::{Duration, SystemTime};

#[derive(Debug, Default)]
pub struct Clock {
    fixed: Mutex<Option<SystemTime>>,
}

impl Clock {
    /// Fixed at `now` if given, else system time.
    pub fn new(now: Option<SystemTime>) -> Self {
        Self {
            fixed: Mutex::new(now),
        }
    }

    pub fn now(&self) -> SystemTime {
        self.guard().unwrap_or_else(SystemTime::now)
    }

    /// Fixes the clock at `t`.
    pub fn set(&self, t: SystemTime) {
        *self.guard() = Some(t);
    }

    /// Fixes the clock `d` after its current reading.
    pub fn advance(&self, d: Duration) {
        let mut fixed = self.guard();
        *fixed = Some(fixed.unwrap_or_else(SystemTime::now) + d);
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, Option<SystemTime>> {
        self.fixed.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_and_advanced() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let c = Clock::new(Some(t));
        assert_eq!(c.now(), t);
        c.advance(Duration::from_secs(5));
        assert_eq!(c.now(), t + Duration::from_secs(5));

        let c = Clock::default();
        let before = SystemTime::now();
        assert!(c.now() >= before);
        c.advance(Duration::from_secs(3600));
        assert!(c.now() >= before + Duration::from_secs(3600));
        c.set(t);
        assert_eq!(c.now(), t);
    }
}
