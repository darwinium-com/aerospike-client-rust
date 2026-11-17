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

use aerospike_rt::time::Instant;
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::{Node, Cluster};
use crate::cluster::partition::Partition;
use crate::commands;
use crate::commands::buffer::Buffer;
use crate::commands::field_type::FieldType;
use crate::net::PooledConnection;
use byteorder::{ByteOrder, NetworkEndian};
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::Connection;
use crate::policy::{BatchPolicy, Policy, PolicyLike, Replica};
use crate::{value, BatchRead, Key, Record, ResultCode, Value};
use aerospike_rt::sleep;

struct BatchRecord {
    batch_index: usize,
    record: Option<Record>,
}

#[derive(Clone, Debug)]
pub struct BatchReadCommand<'a> {
    policy: BatchPolicy,
    pub node: Arc<Node>,
    pub batch_reads: Vec<(BatchRead<'a>, usize)>,
}

impl<'a> BatchReadCommand<'a> {
    pub fn new(policy: &BatchPolicy, node: Arc<Node>, batch_reads: Vec<(BatchRead<'a>, usize)>) -> Self {
        BatchReadCommand {
            policy: policy.clone(),
            node,
            batch_reads,
        }
    }

    pub async fn execute(mut self, cluster: Arc<Cluster>) -> Result<Self> {
        let mut iterations = 0;
        let base_policy = self.policy.base().clone();

        // set timeout outside the loop
        let deadline = base_policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            let success = if iterations & 1 == 0 || matches!(self.policy.replica, Replica::Master) {
                // For even iterations, we request all keys from the same node for efficiency.
                Self::request_group(&mut self.batch_reads, &self.policy, self.node.clone(), deadline).await?
            } else {
                // However, for odd iterations try the second choice for each. Instead of re-sharding the batch (as the second choice may not correspond to the first), just try each by itself.
                let mut all_successful = true;
                for individual_read in self.batch_reads.chunks_mut(1) {
                    // Find somewhere else to try.
                    let partition = Partition::new_by_key(&individual_read[0].0.key);
                    let node = cluster.get_node(&partition, self.policy.replica, Arc::downgrade(&self.node))?;

                    if !Self::request_group(individual_read, &self.policy, node, deadline).await? {
                        all_successful = false;
                        break;
                    }
                }
                all_successful
            };

            if success {
                // command has completed successfully.  Exit method.
                return Ok(self);
            }

            iterations += 1;

            // too many retries
            if let Some(max_retries) = base_policy.max_retries() {
                if iterations > max_retries + 1 {
                    bail!(ErrorKind::Connection(format!(
                        "Timeout after {} tries",
                        iterations
                    )));
                }
            }

            // Sleep before trying again, after the first iteration
            if let Some(sleep_between_retries) = base_policy.sleep_between_retries() {
                sleep(sleep_between_retries).await;
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    bail!(ErrorKind::Connection("Timeout".to_string()));
                }
            }
        }
    }

    async fn request_group(batch_reads: &mut [(BatchRead<'a>, usize)], policy: &BatchPolicy, node: Arc<Node>, deadline: Option<Instant>) -> Result<bool> {
        let mut conn = match commands::single_command::try_with_timeout(deadline, node.get_connection()).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("Node {}: {}", node, err);
                return Ok(false);
            }
        };

        // Serialize the request on the caller's task, directly into the connection's (reused)
        // buffer: this is the only step that needs the borrowed batch reads (operations may
        // borrow from the caller).
        if let Err(err) = conn.buffer.set_batch_read(policy, batch_reads) {
            // Nothing has been sent, so the connection is still clean.
            conn.return_to_pool();
            return Err(err).chain_err(|| "Failed to prepare send buffer");
        }
        conn.buffer.write_timeout(policy.base().timeout());

        // Network I/O and response parsing run on their own task, so the responses from
        // each node are drained concurrently (and on other worker threads) rather than all
        // being polled from the caller's single task. A slow reader makes the server back up
        // its batch response buffers (`batch_index_delay`).
        //
        // The spawned task also runs to completion if the caller's future is dropped (e.g. an
        // application level timeout), so the connection is returned to the pool in a clean
        // state rather than being closed mid-response.
        let handle = aerospike_rt::spawn(send_and_parse(conn, node, batch_reads.len(), deadline));

        #[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
        let result = handle
            .await
            .map_err(|err| ErrorKind::Connection(format!("Batch task failed: {}", err)))?;
        #[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
        let result = handle.await;

        match result? {
            None => Ok(false),
            Some(records) => {
                for ((batch_read, _), record) in batch_reads.iter_mut().zip(records) {
                    batch_read.record = record;
                }
                Ok(true)
            }
        }
    }
}

/// Sends the batch request already serialized in `conn.buffer` and parses the response.
///
/// Returns `Ok(None)` for a retryable failure (the send failed), otherwise the records indexed
/// by their position in the request.
async fn send_and_parse(
    mut conn: PooledConnection,
    node: Arc<Node>,
    record_count: usize,
    deadline: Option<Instant>,
) -> Result<Option<Vec<Option<Record>>>> {
    // Send command.
    if let Err(err) = commands::single_command::try_with_timeout(deadline, conn.flush()).await {
        // IO errors are considered temporary anomalies. Retry.
        // Close socket to flush out possible garbage. Do not put back in pool.
        conn.invalidate();
        warn!("Node {}: {}", node, err);
        return Ok(None);
    }

    let mut records: Vec<Option<Record>> = (0..record_count).map(|_| None).collect();

    // Parse results.
    if let Err(err) = commands::single_command::try_with_timeout(deadline, parse_result(&mut records, &mut conn)).await {
        // close the connection
        // cancelling/closing the batch/multi commands will return an error, which will
        // close the connection to throw away its data and signal the server about the
        // situation. We will not put back the connection in the buffer.
        if commands::keep_connection(&err) {
            conn.buffer.reclaim();
            conn.return_to_pool();
        } else {
            conn.invalidate();
        }
        Err(err)
    } else {
        conn.buffer.reclaim();
        conn.return_to_pool();
        Ok(Some(records))
    }
}

/// Reads the response one group (protocol message) at a time. Each group is read from the
/// socket in a single call and then parsed from memory, rather than issuing several small
/// reads per record and per bin.
async fn parse_result(records: &mut [Option<Record>], conn: &mut Connection) -> Result<()> {
    let mut header = [0u8; 8];
    loop {
        // Read the 8 byte proto header onto the stack so the (possibly large) data buffer
        // is neither resized nor overwritten until the group itself is read.
        conn.read(&mut header).await?;
        let size = (NetworkEndian::read_u64(&header) & 0xFFFF_FFFF_FFFF) as usize;
        if size == 0 {
            continue;
        }

        conn.read_buffer_retain(size).await?;
        if !parse_group(records, &mut conn.buffer)? {
            break;
        }
    }
    Ok(())
}

/// Parses every record in a group held in `buf`. Returns false once the last-record marker
/// has been seen.
fn parse_group(records: &mut [Option<Record>], buf: &mut Buffer) -> Result<bool> {
    while buf.remaining() > 0 {
        match parse_record(buf)? {
            None => return Ok(false),
            Some(batch_record) => {
                let slot = records.get_mut(batch_record.batch_index).ok_or_else(|| {
                    ErrorKind::BadResponse(format!(
                        "Invalid batch index {}",
                        batch_record.batch_index
                    ))
                })?;
                *slot = batch_record.record;
            }
        }
    }
    Ok(true)
}

/// Fails with `BadResponse` unless `len` more bytes are available in `buf`.
fn ensure_remaining(buf: &Buffer, len: usize) -> Result<()> {
    if buf.remaining() < len {
        bail!(ErrorKind::BadResponse(format!(
            "Truncated batch response: needed {} bytes, {} remaining",
            len,
            buf.remaining()
        )));
    }
    Ok(())
}

fn parse_record(buf: &mut Buffer) -> Result<Option<BatchRecord>> {
    ensure_remaining(buf, commands::buffer::MSG_REMAINING_HEADER_SIZE as usize)?;
    let base = buf.data_offset;

    // if cmd is the end marker of the response, do not proceed further
    let info3 = buf.read_u8(Some(base + 3));
    if info3 & commands::buffer::INFO3_LAST == commands::buffer::INFO3_LAST {
        return Ok(None);
    }

    let found_key = match ResultCode::from(buf.read_u8(Some(base + 5))) {
        ResultCode::Ok => true,
        ResultCode::KeyNotFoundError => false,
        rc => bail!(ErrorKind::ServerError(rc)),
    };

    buf.data_offset = base + 6;
    let generation = buf.read_u32(None);
    let expiration = buf.read_u32(None);
    let batch_index = buf.read_u32(None);
    let field_count = buf.read_u16(None) as usize; // almost certainly 0
    let op_count = buf.read_u16(None) as usize;

    let key = parse_key(buf, field_count)?;

    let mut bins: HashMap<String, Value> = if found_key {
        HashMap::with_capacity(op_count)
    } else {
        HashMap::new()
    };

    for _ in 0..op_count {
        ensure_remaining(buf, 8)?;
        let op_size = buf.read_u32(None) as usize;
        buf.skip(1);
        let particle_type = buf.read_u8(None);
        buf.skip(1);
        let name_size = buf.read_u8(None) as usize;

        let particle_bytes_size = op_size
            .checked_sub(4 + name_size)
            .ok_or_else(|| ErrorKind::BadResponse("Invalid batch op size".to_string()))?;
        ensure_remaining(buf, name_size + particle_bytes_size)?;

        if !found_key {
            buf.skip(name_size + particle_bytes_size);
            continue;
        }

        let name = buf.read_str(name_size)?;
        let particle_start = buf.data_offset;
        let value = value::bytes_to_particle(particle_type, buf, particle_bytes_size)?;
        // Always resynchronise on the declared length, whatever the decoder consumed.
        buf.data_offset = particle_start + particle_bytes_size;
        bins.insert(name, value);
    }

    let record = if found_key {
        Some(Record::new(Some(key), bins, generation, expiration))
    } else {
        None
    };
    Ok(Some(BatchRecord {
        batch_index: batch_index as usize,
        record,
    }))
}

/// In-memory equivalent of `StreamCommand::parse_key`.
fn parse_key(buf: &mut Buffer, field_count: usize) -> Result<Key> {
    let mut digest: [u8; 20] = [0; 20];
    let mut namespace = String::new();
    let mut set_name = String::new();
    let mut user_key: Option<Value> = None;

    for _ in 0..field_count {
        ensure_remaining(buf, 4)?;
        let field_len = buf.read_u32(None) as usize;
        if field_len == 0 {
            bail!(ErrorKind::BadResponse("Invalid batch key field".to_string()));
        }
        ensure_remaining(buf, field_len)?;
        let field_end = buf.data_offset + field_len;
        let field_type = buf.read_u8(None);
        let data_len = field_len - 1;

        match field_type {
            x if x == FieldType::DigestRipe as u8 => {
                if data_len != digest.len() {
                    bail!(ErrorKind::BadResponse("Invalid digest length".to_string()));
                }
                digest.copy_from_slice(buf.read_slice(data_len));
            }
            x if x == FieldType::Namespace as u8 => {
                namespace = buf.read_str(data_len)?;
            }
            x if x == FieldType::Table as u8 => {
                set_name = buf.read_str(data_len)?;
            }
            x if x == FieldType::Key as u8 => {
                if data_len == 0 {
                    bail!(ErrorKind::BadResponse("Invalid user key field".to_string()));
                }
                let particle_type = buf.read_u8(None);
                user_key = Some(value::bytes_to_particle(particle_type, buf, data_len - 1)?);
            }
            // Unknown fields are skipped rather than treated as fatal.
            _ => {}
        }
        buf.data_offset = field_end;
    }

    Ok(Key {
        namespace,
        set_name,
        user_key,
        digest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_header(out: &mut Vec<u8>, info3: u8, rc: u8, gen: u32, exp: u32, index: u32, fields: u16, ops: u16) {
        out.push(22); // header size
        out.push(0); // info1
        out.push(0); // info2
        out.push(info3);
        out.push(0); // unused
        out.push(rc);
        out.extend_from_slice(&gen.to_be_bytes());
        out.extend_from_slice(&exp.to_be_bytes());
        out.extend_from_slice(&index.to_be_bytes());
        out.extend_from_slice(&fields.to_be_bytes());
        out.extend_from_slice(&ops.to_be_bytes());
    }

    fn op(out: &mut Vec<u8>, name: &str, particle_type: u8, data: &[u8]) {
        out.extend_from_slice(&((4 + name.len() + data.len()) as u32).to_be_bytes());
        out.push(1); // op type
        out.push(particle_type);
        out.push(0); // version
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(data);
    }

    fn buffer(data: Vec<u8>) -> Buffer {
        let mut buf = Buffer::new(1024);
        buf.data_buffer = data;
        buf.reset_offset();
        buf
    }

    #[test]
    fn parses_group_with_found_missing_and_key_fields() {
        let mut data = Vec::new();
        // Record at index 2: found, with a namespace field, an int bin and a string bin.
        record_header(&mut data, 0, 0, 7, 99, 2, 1, 2);
        data.extend_from_slice(&(1 + 4u32).to_be_bytes());
        data.push(FieldType::Namespace as u8);
        data.extend_from_slice(b"test");
        op(&mut data, "i", 1, &42i64.to_be_bytes());
        op(&mut data, "s", 3, b"hello");
        // Record at index 0: not found.
        record_header(&mut data, 0, 2, 0, 0, 0, 0, 0);

        let mut records = vec![None, None, None];
        let mut buf = buffer(data);
        assert!(parse_group(&mut records, &mut buf).unwrap());

        assert!(records[0].is_none());
        assert!(records[1].is_none());
        let rec = records[2].as_ref().unwrap();
        assert_eq!(rec.generation, 7);
        assert_eq!(rec.bins.get("i"), Some(&Value::Int(42)));
        assert_eq!(rec.bins.get("s"), Some(&Value::String("hello".into())));
        assert_eq!(rec.key.as_ref().unwrap().namespace, "test");
    }

    #[test]
    fn stops_at_last_marker() {
        let mut data = Vec::new();
        record_header(&mut data, commands::buffer::INFO3_LAST, 0, 0, 0, 0, 0, 0);
        let mut records: Vec<Option<Record>> = vec![None];
        assert!(!parse_group(&mut records, &mut buffer(data)).unwrap());
    }

    #[test]
    fn rejects_truncated_and_out_of_range() {
        let mut data = Vec::new();
        record_header(&mut data, 0, 0, 0, 0, 0, 0, 1);
        op(&mut data, "i", 1, &42i64.to_be_bytes());
        data.truncate(data.len() - 3);
        let mut records: Vec<Option<Record>> = vec![None];
        assert!(parse_group(&mut records, &mut buffer(data)).is_err());

        let mut data = Vec::new();
        record_header(&mut data, 0, 0, 0, 0, 5, 0, 0);
        assert!(parse_group(&mut records, &mut buffer(data)).is_err());
    }
}
