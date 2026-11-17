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

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::batch::BatchRead;
use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::BatchReadCommand;
use crate::errors::Result;
use crate::policy::BatchPolicy;
use crate::Key;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

const MAX_BATCH_REQUEST_SIZE : usize = 5000;

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }

    pub async fn execute_batch_read<'a>(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead<'a>>,
    ) -> Result<Vec<BatchRead<'a>>> {
        let record_count = batch_reads.len();
        let batch_nodes = self.get_batch_nodes(batch_reads, policy.replica)?;
        let mut jobs = Vec::<BatchReadCommand<'a>>::new();
        for (node, mut node_jobs) in batch_nodes {
            // Split into request sized chunks by moving, not cloning, the batch reads.
            while node_jobs.len() > MAX_BATCH_REQUEST_SIZE {
                let rest = node_jobs.split_off(MAX_BATCH_REQUEST_SIZE);
                jobs.push(BatchReadCommand::new(policy, node.clone(), node_jobs));
                node_jobs = rest;
            }
            jobs.push(BatchReadCommand::new(policy, node, node_jobs));
        }
        let reads = self.execute_batch_jobs(jobs).await?;

        // Put each result back at its original position (O(n), no sort).
        let mut ordered: Vec<Option<BatchRead<'a>>> = (0..record_count).map(|_| None).collect();
        for (batch_read, index) in reads.into_iter().flat_map(|cmd| cmd.batch_reads) {
            ordered[index] = Some(batch_read);
        }
        Ok(ordered
            .into_iter()
            .map(|batch_read| batch_read.expect("every batch read is assigned to exactly one job"))
            .collect())
    }

    async fn execute_batch_jobs<'a>(
        &self,
        jobs: Vec<BatchReadCommand<'a>>,
    ) -> Result<Vec<BatchReadCommand<'a>>> {
        let handles = jobs.into_iter().map(|job: BatchReadCommand<'a>|job.execute(self.cluster.clone()));
        futures::future::join_all(handles).await.into_iter().collect()
    }

    fn get_batch_nodes<'a>(
        &self,
        batch_reads: Vec<BatchRead<'a>>,
        replica: crate::policy::Replica,
    ) -> Result<HashMap<Arc<Node>, Vec<(BatchRead<'a>, usize)>>> {
        let mut map = HashMap::new();
        for (index, batch_read) in batch_reads.into_iter().enumerate() {
            let node = self.node_for_key(&batch_read.key, replica)?;
            map.entry(node)
                .or_insert_with(Vec::new)
                .push((batch_read, index));
        }
        Ok(map)
    }

    fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition, replica, Weak::new())?;
        Ok(node)
    }
}
