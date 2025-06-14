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

use thiserror::Error;

#[derive(Error, Debug, Eq, PartialEq, Clone)]
pub enum DatabaseError {
    #[error("data buffer is too small, expect {expect}, got {got}")]
    TooSmallData { expect: usize, got: usize },

    #[error("file not found: {0}")]
    FileNotFound(String),

    #[error("could not operate on file {0}: {1}")]
    IOError(String, String),

    #[error("page {id} type is invalid, expect {expect}, got {got}")]
    InvalidPageType { expect: u16, got: u16, id: u64 },

    #[error("page {id} checksum is invalid, expect {expect}, got {got}")]
    InvalidPageChecksum { expect: u64, got: u64, id: u64 },

    #[error("page {id} magic is invalid, expect {expect}, got {got}")]
    InvalidPageMagic { expect: u32, got: u32, id: u64 },

    #[error("page {id} version is invalid, expect {expect}, got {got}")]
    InvalidPageVersion { expect: u32, got: u32, id: u64 },

    #[error("file's meta is invalid")]
    InvalidMeta,
}
