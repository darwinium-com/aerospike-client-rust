// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::cluster::{Cluster, Node};
use crate::commands::{Command, SingleCommand};
use crate::errors::Result;
use crate::net::Connection;
use crate::operations::Operation;
use crate::policy::WritePolicy;
use crate::{Key, Record, Value};

use super::read_command;

pub struct OperateCommand<'a, T: serde::de::DeserializeOwned + Send> {
    pub single_command: SingleCommand<'a>,
    policy: &'a WritePolicy,
    operations: &'a [Operation<'a>],
    phantom: PhantomData<T>,
}

/// The return value from operate. Like a record, but retains the order and duplicate keys of bins.
#[derive(Default)]
pub struct OperateRecord {
    /// Record key. When reading a record from the database, the key is not set in the returned
    /// Record struct.
    pub key: Option<Key>,

    /// Map of named record bins.
    pub bins: Vec<(String, Value)>,

    /// Record modification count.
    pub generation: u32,

    /// Date record will expire, in seconds from Jan 01 2010, 00:00:00 UTC.
    expiration: u32,
}

impl<'a, T: serde::de::DeserializeOwned + Send> OperateCommand<'a, T> {
    pub fn new(
        policy: &'a WritePolicy,
        cluster: Arc<Cluster>,
        key: &'a Key,
        operations: &'a [Operation<'a>],
    ) -> Self {
        OperateCommand {
            single_command: SingleCommand::new(cluster, key, crate::policy::Replica::Master),
            policy,
            operations,
            phantom: Default::default(),
        }
    }

    pub async fn execute(&mut self) -> Result<<Self as Command>::Output> {
        SingleCommand::execute(self.policy, self).await
    }
}

#[async_trait::async_trait]
impl<'a, T: serde::de::DeserializeOwned + Send> Command for OperateCommand<'a, T> {
    type Output = Record<T>;

    async fn write_timeout(
        &mut self,
        conn: &mut Connection,
        timeout: Option<Duration>,
    ) -> Result<()> {
        conn.buffer.write_timeout(timeout);
        Ok(())
    }

    fn prepare_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.buffer.set_operate(
            self.policy,
            self.single_command.key,
            self.operations,
        )
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<Self::Output> {
        read_command::ReadCommand::parse_result_internal(conn, false).await
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }
}
