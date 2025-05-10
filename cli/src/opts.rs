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

use clap::Args;
use clap_verbosity_flag::{LogLevel, VerbosityFilter};
use cling::prelude::*;

#[derive(Args, Collect, Clone, Default)]
pub struct CommonOpts {
    #[clap(flatten)]
    pub(crate) verbose: clap_verbosity_flag::Verbosity<Quiet>,

    #[arg(long)]
    pub(crate) db: String,
}

#[derive(Clone, Default)]
pub(crate) struct Quiet;
impl LogLevel for Quiet {
    fn default_filter() -> VerbosityFilter {
        VerbosityFilter::Error
    }

    fn verbose_long_help() -> Option<&'static str> {
        None
    }

    fn quiet_help() -> Option<&'static str> {
        None
    }

    fn quiet_long_help() -> Option<&'static str> {
        None
    }
}
