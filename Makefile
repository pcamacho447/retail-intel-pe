.PHONY: verify verify-rust verify-py

verify: verify-rust verify-py

verify-rust:
	cd rust-ingestor && cargo fmt --check && cargo clippy --all-targets --locked -- -D warnings && cargo test --locked

verify-py:
	.venv/bin/pytest -q
