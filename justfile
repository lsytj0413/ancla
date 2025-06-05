export RUST_BACKTRACE := env_var_or_default("RUST_BACKTRACE", "short")

features := ""
libc := "gnu"
arch := "" # use the default architecture
os := "" # use the default os

_features := if features == "all" {
    "--all-features"
} else if features != "" {
    "--features=" + features
} else {
    ""
}

_arch := if arch == "" {
    arch()
} else if arch == "x86_64" {
    "x86_64"
} else if arch == "arm64" {
    "aarch64"
} else if arch == "aarch64" {
    "aarch64"
} else {
    error("unsupported arch=" + arch)
}

_os := if os == "" {
    os()
} else {
    os
}

_os_target := if _os == "macos" {
    "apple-darwin"
} else if _os == "linux" {
    "unknown-linux"
} else {
    error("unsupported os=" + _os)
}

_default_target := `rustc -vV | sed -n 's|host: ||p'`
target := _arch + "-" + _os_target + if _os == "linux" { "-" + libc } else { "" }
_resolved_target := if target != _default_target { target } else { "" }
_target-option := if _resolved_target != "" { "--target " + _resolved_target } else { "" }

_flamegraph_options := if os() == "macos" { "--root" } else { "" }

clean:
    cargo clean

fmt:
    cargo fmt --all

check-fmt:
    cargo fmt --all -- --check

clippy: (_target-installed target)
    cargo hack clippy {{ _target-option }} --all-targets --workspace --each-feature -- -D warnings

lint: check-fmt clippy

build *flags: (_target-installed target)
    cargo build {{ _target-option }} {{ _features }} {{ flags }}

print-target:
    @echo {{ _resolved_target }}

run *flags: (_target-installed target)
    cargo run {{ _target-option }} {{ flags }}

test: (_target-installed target)
    cargo test --all-features --target-dir target/tests

flamegraph *flags:
    cargo flamegraph {{ _flamegraph_options }} {{ flags }}

_target-installed target:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! rustup target list --installed | grep -qF '{{ target }}' 2>/dev/null ; then
        rustup target add '{{ target }}'
    fi

verify: lint test
