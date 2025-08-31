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
use crate::policy::{BatchPolicy, Concurrency};
use crate::Key;

pub struct BatchExecutor {
    cluster: Arc<Cluster>,
}

const MAX_BATCH_REQUEST_SIZE : usize = 5000;

impl BatchExecutor {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        BatchExecutor { cluster }
    }


    pub async fn execute_batch_read<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead<T>>,
    ) -> Result<Vec<BatchRead<T>>> {
        let total = batch_reads.len();
        let jobs = self.get_batch_nodes(policy, batch_reads)?;
        let reads = self.execute_batch_jobs::<T>(jobs, policy.concurrency).await?;

        let mut as_iter = reads.into_iter();
        if let Some(BatchReadCommand { mut batch_reads, mut original_indexes, .. }) = as_iter.next() {
            // Reserve enough to make the first element the return value 
            batch_reads.reserve_exact(total - batch_reads.len());
            original_indexes.reserve_exact(total - original_indexes.len());
            // Shove everything into the same list
            for another_job in as_iter {
                batch_reads.extend(another_job.batch_reads);
                original_indexes.extend(another_job.original_indexes);
            }

            // Put records back where it belongs... this is 0(n) because everything is swapped into its correct position
            for i in 0..batch_reads.len() {
                while original_indexes[i] != i {
                    let to = original_indexes[i];
                    batch_reads.swap(i, to);
                    original_indexes.swap(i, to);
                }
            }
            Ok(batch_reads)
        } else {
            Ok(Default::default())
        }
    }

    async fn execute_batch_jobs<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        jobs: Vec<BatchReadCommand<T>>,
        concurrency: Concurrency,
    ) -> Result<Vec<BatchReadCommand<T>>> {
        let handles = jobs.into_iter().map(|job|job.execute(self.cluster.clone()));
        match concurrency {
            Concurrency::Sequential => futures::future::join_all(handles).await.into_iter().collect(),
            Concurrency::Parallel => futures::future::join_all(handles.map(aerospike_rt::spawn)).await.into_iter().map(|value|value.map_err(|e|e.to_string())?).collect(),
        }
    }

    fn get_batch_nodes<'l, T: serde::de::DeserializeOwned + Send>(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead<T>>,
    ) -> Result<Vec<BatchReadCommand<T>>> {
        let mut map: HashMap<Arc<Node>, (Vec<BatchRead<T>>, Vec<usize>)> = HashMap::new();
        let mut vec = Vec::new();
        let choices = batch_reads.first().map(|read|self.cluster.n_nodes_for_policy(&read.key.namespace, policy.replica)).unwrap_or_default();
        vec.reserve(choices);
        let estimate = batch_reads.len() / (choices.max(2) - 1);

        for (index, batch_read) in batch_reads.into_iter().enumerate() {
            let node = self.node_for_key(&batch_read.key, policy.replica)?;
            let (reads, indexes) = map.entry(node)
                .or_insert_with(||{
                    let mut reads = Vec::new();
                    let mut indexes = Vec::new();
                    if estimate > MAX_BATCH_REQUEST_SIZE {
                        reads.reserve_exact(MAX_BATCH_REQUEST_SIZE);
                        indexes.reserve_exact(MAX_BATCH_REQUEST_SIZE);
                    } else {
                        reads.reserve(estimate);
                        indexes.reserve(estimate);
                    }
                    (reads, indexes)
                });
            
            // Enough reads, make a new one.
            if reads.len() >= MAX_BATCH_REQUEST_SIZE {
                // To avoid copying node above, we just re-do node from key when it's needed.
                let node = self.node_for_key(&batch_read.key, policy.replica)?;
                vec.push(BatchReadCommand::new(policy, node, std::mem::take(reads), std::mem::take(indexes)));
                // If we're blowing out buffers, we'll probably do it again.
                reads.reserve_exact(MAX_BATCH_REQUEST_SIZE);
                indexes.reserve_exact(MAX_BATCH_REQUEST_SIZE);
            }
            reads.push(batch_read);
            indexes.push(index);
        }

        vec.reserve_exact(map.len());
        for (node, (reads, indexes)) in map {
            vec.push(BatchReadCommand::new(policy, node, reads, indexes));
        }
        Ok(vec)
    }

    fn node_for_key(&self, key: &Key, replica: crate::policy::Replica) -> Result<Arc<Node>> {
        let partition = Partition::new_by_key(key);
        let node = self.cluster.get_node(&partition, replica, Weak::new())?;
        Ok(node)
    }
}
