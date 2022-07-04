default: check

fmt_check:
	cargo fmt --all -- --check

fmt:
	cargo fmt --all

clippy_check:
	cargo clippy --all-targets --all-features --locked -- -D warnings

clippy:
	cargo clippy --all-features --fix --allow-dirty --allow-staged

build:
	cargo build --all-features

test:
	cargo nextest run --no-fail-fast --all-features --locked

check: fmt_check clippy_check build test

clean:
	cargo clean

run:
	cargo run --release
