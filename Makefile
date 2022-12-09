default: check

fmt_check:
	cargo fmt --all -- --check

fmt:
	cargo fmt --all

clippy_check:
	cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

clippy:
	cargo clippy --workspace --all-targets --all-features --fix --allow-dirty --allow-staged

build:
	cargo build --all-targets --all-features

planner_test_build:
	cargo run -p sqlplannertest-test --bin apply

planner_test:
	cargo nextest run -p sqlplannertest-test

test:
	cargo nextest run --workspace --no-fail-fast --all-features --locked

check: fmt_check clippy_check build test

clean:
	cargo clean

run:
	cargo run --release

debug:
	RUST_BACKTRACE=1 cargo run

debug_v2:
	ENABLE_V2=1 RUST_BACKTRACE=1 cargo run
