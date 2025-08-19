use crate::cache::DEFAULT_PAGE_CACHE_SIZE;

use std::sync::LazyLock;

#[allow(non_snake_case)]
pub struct Config {
    // number of pages in cache
    pub PAGE_CACHE_SIZE: usize,
}

pub static CONFIG: LazyLock<Config> = LazyLock::new(|| Config {
    PAGE_CACHE_SIZE: DEFAULT_PAGE_CACHE_SIZE,
});
