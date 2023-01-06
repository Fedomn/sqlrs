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

run_v2:
	ENABLE_V2=1 cargo run --release

debug:
	RUST_BACKTRACE=1 cargo run

debug_v2:
	ENABLE_V2=1 RUST_BACKTRACE=1 cargo run

debug_v2_log:
	RUST_LOG='sqlrs::planner=debug,sqlrs::execution=debug' ENABLE_V2=1 RUST_BACKTRACE=1 cargo run

TPCH_DBGEN_PATH := tpch-dbgen
TPCH_DBGEN_DATA := tpch-data
TPCH_SCALE := 1

$(TPCH_DBGEN_PATH):
	mkdir -p target
	git clone https://github.com/electrum/tpch-dbgen.git $(TPCH_DBGEN_PATH)

tpch: $(TPCH_DBGEN_PATH)
	make -C $(TPCH_DBGEN_PATH)
	cd $(TPCH_DBGEN_PATH) && ./dbgen -f -s $(TPCH_SCALE) && mkdir -p tbl && mv *.tbl tbl
	make tpch_gen_query
	make tpch_collect_data

tpch_gen_query:
	cd $(TPCH_DBGEN_PATH) && \
	mkdir -p ./gen-queries && \
	export DSS_QUERY=./queries && \
	for ((i=1;i<=22;i++)); do ./qgen -v -d -c -s $${i} $${i} > ./gen-queries/tpch-q$${i}.sql; done

tpch_collect_data:
	mkdir -p $(TPCH_DBGEN_DATA)
	mv $(TPCH_DBGEN_PATH)/tbl $(TPCH_DBGEN_DATA)
	mv $(TPCH_DBGEN_PATH)/gen-queries $(TPCH_DBGEN_DATA)
	cp -r $(TPCH_DBGEN_PATH)/answers $(TPCH_DBGEN_DATA)

tpch_clean:
	rm -rf $(TPCH_DBGEN_PATH)
	rm -rf $(TPCH_DBGEN_DATA)
