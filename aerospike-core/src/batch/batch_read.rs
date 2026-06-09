// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use crate::Bins;
use crate::Key;
use crate::Record;
use crate::operations::Operation;

#[derive(Debug, Clone)]
pub enum BatchReadSource<'a> {
    Bin(Bins),
    Ops(Vec<Operation<'a>>),
    OpsSlice(&'a [Operation<'a>]),
}

/// Key and bin names used in batch read commands where variable bins are needed for each key.
#[derive(Debug, Clone)]
pub struct BatchRead<'a> {
    /// Key.
    pub key: Key,

    /// Batch read source, bins or ops
    pub source: BatchReadSource<'a>,

    /// Will contain the record after the batch read operation.
    pub record: Option<Record>,
}

impl<'a> BatchRead<'a> {
    /// Create a new `BatchRead` instance for the given key and bin selector.
    pub const fn new(key: Key, bins: Bins) -> Self {
        BatchRead {
            key,
            source: BatchReadSource::Bin(bins),
            record: None,
        }
    }

    /// Create a new `BatchRead` instance for the given key and ops.
    pub fn with_ops(key: Key, ops: Vec<Operation<'a>>) -> Self {
        BatchRead {
            key,
            source: BatchReadSource::Ops(ops),
            record: None,
        }
    }

    /// Create a new `BatchRead` instance for the given key and ops.
    /// If consecutive batch reads refer to the exact same ops slice the wire
    /// protocol will only send the ops once for that set of batch reads.
    /// This can result in substantial savings if operations are large.
    pub fn with_ops_slice(key: Key, ops: &'a [Operation<'a>]) -> Self {
        BatchRead {
            key,
            source: BatchReadSource::OpsSlice(ops),
            record: None,
        }
    }

    #[doc(hidden)]
    pub fn match_header(&self, other: &BatchRead<'a>, match_set: bool) -> bool {
        let key = &self.key;
        let other_key = &other.key;

        let source_matches = match (&self.source, &other.source) {
            (BatchReadSource::Bin(lhs), BatchReadSource::Bin(rhs)) => lhs == rhs,
            (BatchReadSource::OpsSlice(lhs), BatchReadSource::OpsSlice(rhs)) => std::ptr::eq(*lhs, *rhs),
            _ => false, // deep operation comparison is probably not worth it
        };

        source_matches &&
            (key.namespace == other_key.namespace)
            && (!match_set || (key.set_name == other_key.set_name))
    }
}

