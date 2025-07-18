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

use anyhow::Result;
use clap::Parser;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "run_get")]
pub struct Get {
    #[clap(long)]
    pub buckets: Vec<String>,
    #[clap(long)]
    pub key: String,
}

pub fn run_get(
    state: State<crate::cli_env::Env>,
    args: &Get,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    println!("{args:?}");

    let kv = state.0.db.get_key_value(&args.buckets, &args.key);
    if let Some(kv) = kv {
        println!("Key: {:?}", String::from_utf8(kv.key));
        println!("Value: {:?}", String::from_utf8(kv.value));
    } else {
        println!("Key not found");
    }
    Ok(())
}
