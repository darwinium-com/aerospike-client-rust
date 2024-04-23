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

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use crate::cluster::{Cluster, Node};
use crate::commands::buffer;
use crate::commands::{Command, SingleCommand};
use crate::errors::{ErrorKind, Result};
use crate::net::Connection;
use crate::policy::{BasePolicy, Replica};
use crate::{derive, Bins, Key, Record, ResultCode};

pub struct ReadCommand<'a, T: serde::de::DeserializeOwned + Send> {
    pub single_command: SingleCommand<'a>,
    pub record: Option<Record<T>>,
    policy: &'a BasePolicy,
    bins: Bins,
}

impl<'a, T: serde::de::DeserializeOwned + Send> ReadCommand<'a, T> {
    pub fn new(policy: &'a BasePolicy, cluster: Arc<Cluster>, key: &'a Key, bins: Bins, replica: Replica) -> Self {
        ReadCommand {
            single_command: SingleCommand::new(cluster, key, replica),
            bins,
            policy,
            record: None,
        }
    }

    pub async fn execute(&mut self) -> Result<<Self as Command>::Output> {
        SingleCommand::execute(self.policy, self).await
    }

    pub async fn parse_record(
        conn: &mut Connection,
        op_count: usize,
        field_count: usize,
        generation: u32,
        expiration: u32,
    ) -> Result<Record<T>> {
        // There can be fields in the response (setname etc). For now, ignore them. Expose them to
        // the API if needed in the future.
        for _ in 0..field_count {
            conn.read_buffer(4).await?;
            let field_size = conn.buffer.read_u32(None) as usize;
            conn.read_buffer(field_size).await?;
            conn.buffer.skip(field_size);
        }

        let reader = crate::derive::readable::BinsDeserializer{ bins: conn.pre_parse_stream_bins(op_count).await?.into() };

        let bins = T::deserialize(reader)?;
        Ok(Record::new(None, bins, generation, expiration))
    }

    pub async fn parse_result_internal(conn: &mut Connection, bins_none: bool) -> Result<Record<T>> {
        if let Err(err) = conn
            .read_buffer(buffer::MSG_TOTAL_HEADER_SIZE as usize)
            .await
        {
            warn!("Parse result error: {}", err);
            bail!(err);
        }

        conn.buffer.reset_offset();
        conn.buffer.skip(9);
        let result_code = conn.buffer.read_u8(Some(13));
        let generation = conn.buffer.read_u32(Some(14));
        let expiration = conn.buffer.read_u32(Some(18));
        let field_count = conn.buffer.read_u16(Some(26)) as usize; // almost certainly 0
        let op_count = conn.buffer.read_u16(Some(28)) as usize;

        match ResultCode::from(result_code) {
            ResultCode::Ok => {
                let record = if bins_none {
                    Record::new(None, T::deserialize(derive::readable::BinsDeserializer{bins: VecDeque::new()})?, generation, expiration)
                } else {
                    Self::parse_record(conn, op_count, field_count, generation, expiration)
                        .await?
                };
                Ok(record)
            }
            ResultCode::UdfBadResponse => {
                // record bin "FAILURE" contains details about the UDF error
                let reason = parse_udf_error(conn, op_count, field_count).await?;
                Err(ErrorKind::UdfBadResponse(reason).into())
            }
            rc => Err(ErrorKind::ServerError(rc).into()),
        }
    }
}

pub(crate) async fn parse_udf_error(
    conn: &mut Connection,
    op_count: usize,
    field_count: usize,
) -> Result<String> {
    // There can be fields in the response (setname etc). For now, ignore them. Expose them to
    // the API if needed in the future.
    for _ in 0..field_count {
        conn.read_buffer(4).await?;
        let field_size = conn.buffer.read_u32(None) as usize;
        conn.read_buffer(field_size).await?;
        conn.buffer.skip(field_size);
    }

    let reader = crate::derive::readable::BinsDeserializer{ bins: conn.pre_parse_stream_bins(op_count).await?.into() };

    #[derive(Deserialize)]
    struct FailureReason {
        #[serde(rename = "FAILURE")]
        failure: Option<String>,
    }
    let bins = FailureReason::deserialize(reader)?;
    if let Some(fail) = bins.failure {
        return Ok(fail);
    }
    Ok(String::from("UDF Error"))
}

#[async_trait::async_trait]
impl<'a, T: serde::de::DeserializeOwned + Send> Command for ReadCommand<'a, T> {
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
        conn.buffer
            .set_read(self.policy, self.single_command.key, &self.bins)
    }

    fn get_node(&mut self) -> Result<Arc<Node>> {
        self.single_command.get_node()
    }

    async fn parse_result(&mut self, conn: &mut Connection) -> Result<Record<T>> {
        Self::parse_result_internal(conn, self.bins.is_none()).await
    }

    async fn write_buffer(&mut self, conn: &mut Connection) -> Result<()> {
        conn.flush().await
    }
}
