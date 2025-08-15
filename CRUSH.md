# CRUSH.md - Development Guidelines for JoujouDB

## Build Commands
- `cargo build` - Build the project
- `cargo build --release` - Build with optimizations

## Test Commands
- `cargo test` - Run all tests
- `cargo test test_name` - Run a specific test
- `cargo test module_name` - Run tests for a specific module
- `cargo test --lib` - Run only library tests
- `cargo test -- --nocapture` - Run tests with output displayed

## Lint/Format Commands
- `cargo fmt` - Format code according to Rust standards
- `cargo fmt -- --check` - Check if code is properly formatted
- `cargo clippy` - Run clippy lints
- `cargo clippy --fix` - Automatically fix clippy issues

## Code Style Guidelines
- Use Rust standard formatting (run `cargo fmt`)
- Follow clippy recommendations for idiomatic Rust
- Use `thiserror` for error handling as seen in the codebase
- Import modules using `pub mod` in lib.rs
- Use descriptive names for structs, enums, and functions
- Keep functions small and focused on a single responsibility
- Use `zerocopy` and `zerocopy-derive` for efficient data structures as seen in the codebase
- Organize code in modules by functionality (cache, indexes, pages, etc.)

## Types and Naming Conventions
- Use CamelCase for struct and enum names
- Use snake_case for function and variable names
- Use descriptive error names with `Error` suffix
- Use `PageId` style for identifiers

## Error Handling
- Use `thiserror` for defining errors
- Return `Result` types for functions that can fail
- Handle errors explicitly rather than panicking
