# Contributing to Kuba TSDB

Thank you for your interest in contributing to Kuba TSDB. This document outlines the contribution process and development guidelines.

## Getting Started

### Prerequisites

- **Rust**: 1.70 or later (stable channel)
- **Redis**: 6.0+ (optional, for integration tests with Redis features)
- **Git**: For version control

### Development Setup

```bash
# Clone the repository
git clone https://github.com/victoralfred/kuba-tsdb.git
cd kuba-tsdb/source

# Build the project
cargo build

# Run tests
cargo test

# Run with all features
cargo test --all-features
```

## Development Workflow

### 1. Find or Create an Issue

Before starting work:
- Check existing issues for the feature or bug
- For new features, open an issue first to discuss the approach
- For bugs, ensure the issue includes reproduction steps

### 2. Create a Branch

```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Or for bug fixes
git checkout -b fix/issue-description
```

### 3. Make Your Changes

Follow these guidelines:

#### Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` and address all warnings
- Follow Rust naming conventions (snake_case for functions, CamelCase for types)

#### Documentation

- Add doc comments (`///`) for public APIs
- Include examples in doc comments where helpful
- Update README.md if adding user-facing features

#### Testing

- Write unit tests for new functionality
- Add integration tests for complex features
- Ensure all existing tests pass

```bash
# Run all tests
cargo test

# Run specific test module
cargo test compression::

# Run benchmarks
cargo bench
```

### 4. Commit Your Changes

Write clear, descriptive commit messages:

```
Add parallel chunk compression for concurrent sealing

Implement multi-threaded compression using Rayon to improve
seal throughput. Benchmarks show 2.5x improvement on 8-core
systems with multiple active chunks.
```

### 5. Submit a Pull Request

- Push your branch to your fork
- Open a pull request against `main`
- Fill out the PR template completely
- Link related issues

## Code Guidelines

### Performance

Kuba TSDB is a high-performance database. Keep these principles in mind:

- **Avoid allocations in hot paths**: Use pre-allocated buffers where possible
- **Prefer zero-copy operations**: Use `&[u8]` slices and memory mapping
- **Benchmark significant changes**: Use `cargo bench` to measure impact
- **Consider cache efficiency**: Group related data together

### Safety

- Use `unsafe` only when necessary and document the safety invariants
- Validate all external input (network, files, user data)
- Handle errors explicitly; avoid `unwrap()` in library code
- Test edge cases and error conditions

### Architecture

- Follow the pluggable trait pattern (Compressor, StorageEngine, TimeIndex)
- Keep modules focused and cohesive
- Minimize dependencies between modules
- Use async/await for I/O-bound operations

## Testing Guidelines

### Unit Tests

Place unit tests in the same file as the code:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_roundtrip() {
        // Test implementation
    }
}
```

### Integration Tests

Place integration tests in the `tests/` directory for cross-module testing.

### Property-Based Tests

Use `proptest` for testing invariants across random inputs:

```rust
proptest! {
    #[test]
    fn compression_preserves_data(points: Vec<DataPoint>) {
        // Property test implementation
    }
}
```

## Pull Request Checklist

Before submitting:

- [ ] Code compiles without warnings (`cargo build`)
- [ ] All tests pass (`cargo test`)
- [ ] Clippy passes (`cargo clippy`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] PR description explains the changes

## Review Process

1. A maintainer will review your PR within a few days
2. Address any requested changes
3. Once approved, a maintainer will merge the PR

## Reporting Bugs

When reporting bugs, include:

- Kuba TSDB version
- Rust version (`rustc --version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

## Feature Requests

For feature requests, describe:

- The use case or problem being solved
- Proposed solution (if any)
- Alternatives considered
- Impact on performance or existing functionality

## Questions

- Open a GitHub issue for technical questions
- Check existing issues and documentation first

## License

By contributing to Kuba TSDB, you agree that your contributions will be licensed under the MIT License.
