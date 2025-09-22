use crate::cache::DEFAULT_PAGE_CACHE_SIZE;

use std::{sync::LazyLock, time::Duration};

#[allow(non_snake_case)]
pub struct Config {
    // number of pages in cache
    pub PAGE_CACHE_SIZE: usize,
    // root directory
    pub ROOT_DIRECTORY: String,
    // interval between pagecache write back to storage
    pub WRITEBACK_INTERVAL_MS: Duration,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| Config {
    PAGE_CACHE_SIZE: DEFAULT_PAGE_CACHE_SIZE,
    ROOT_DIRECTORY: "/tmp/joujoudb".to_string(),
    WRITEBACK_INTERVAL_MS: Duration::from_millis(50),
});
