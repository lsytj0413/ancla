// MIT License
// 
// Copyright (c) 2024 Songlin Yang
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! Build information

use std::sync::OnceLock;

/// It's the version of current ancla-cli, same as `package.version` in `Cargo.toml`.
/// and it's value MUST obey the [Semantic Versioning](https://semver.org/) rules.
/// Example: `0.1.0-alpha.1`
pub const ANCLA_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

/// It's the major part of version, for example, `0` in `0.1.0-alpha.1`.
pub const ANCLA_CLI_VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
/// It's the minor part of version, for example, `1` in `0.1.0-alpha.1`.
pub const ANCLA_CLI_VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
/// It's the patch part of version, for example, `0` in `0.1.0-alpha.1`.
pub const ANCLA_CLI_VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");
/// It's the pre part of version, for example, `alpha.1` in `0.1.0-alpha.1`.
pub const ANCLA_CLI_VERSION_PRE: &str = env!("CARGO_PKG_VERSION_PRE");

/// It's the date of the build, for example, `2025-04-30`.
pub const ANCLA_CLI_BUILD_DATE: &str = env!("VERGEN_BUILD_DATE");
/// It's the time of the build, for example, `2025-04-30T02:08:48.979612094Z`.
pub const ANCAL_CLI_BUILD_TIME: &str = env!("VERGEN_BUILD_TIMESTAMP");
/// It's the short style commit SHA of the build, for example, `00c4dd4`.
pub const ANCLA_CLI_COMMIT_SHA: &str = env!("VERGEN_GIT_SHA");
/// It's the date of the commit, for example, `2025-04-28`.
pub const ANCLA_CLI_COMMIT_DATE: &str = env!("VERGEN_GIT_COMMIT_DATE");
/// It's the branch of the commit, for example, `master`.
pub const ANCLA_CLI_BRANCH: &str = env!("VERGEN_GIT_BRANCH");

/// It's the target of the build, for example, `aarch64-unknown-linux-gnu`.
pub const ANCLA_CLI_TARGET_TRIPLE: &str = env!("VERGEN_CARGO_TARGET_TRIPLE");
/// It's the selected features when build.
/// for example, if build with `--features=f1 --features=f2`, the value is `f1,f2`.
pub const ANCLA_CLI_BUILD_FEATURES: &str = env!("VERGEN_CARGO_FEATURES");

fn build_info() -> String {
    format!(
        "{ANCLA_CLI_VERSION}{} ({ANCLA_CLI_COMMIT_SHA} {ANCLA_CLI_TARGET_TRIPLE} {ANCLA_CLI_BUILD_DATE})",
        if is_debug() {" (debug)" } else {""}
    )
}

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version() -> &'static str {
    VERSION.get_or_init(build_info)
}

/// When build with env `DEBUG_TRIPPED="true"`, this value is `true`,
/// and use MUST use `objcopy --strip-debug` to optimize binary size.
const ANCLA_CLI_DEBUG_STRIPPED: Option<&str> = option_env!("DEBUG_STRIPPED");

/// It's true when build without `--release`.
const ANCLA_CLI_DEBUG: &str = env!("VERGEN_CARGO_DEBUG");

fn is_debug() -> bool {
    ANCLA_CLI_DEBUG == "true" && ANCLA_CLI_DEBUG_STRIPPED != Some("true")
}
