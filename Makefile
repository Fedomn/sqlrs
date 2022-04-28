fmt_check:
	cargo fmt --all -- --check

fmt:
	cargo fmt --all

clippy_check:
	cargo clippy --workspace --all-features

clippy:
	cargo clippy --workspace --all-features --fix

build:
	cargo build --all-features

test:
	cargo test --workspace --all-features

check: fmt_check clippy_check build test

clean:
	cargo clean
