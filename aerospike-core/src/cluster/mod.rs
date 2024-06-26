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

pub mod node;
pub mod node_validator;
pub mod partition;
pub mod partition_tokenizer;

use aerospike_rt::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::{Arc, Weak};
use std::vec::Vec;

pub use self::node::Node;

use self::node_validator::NodeValidator;
use self::partition::Partition;
use self::partition_tokenizer::PartitionTokenizer;

use crate::commands::Message;
use crate::errors::{ErrorKind, Result};
use crate::net::Host;
use crate::policy::ClientPolicy;
use std::sync::Mutex;
use futures::channel::mpsc;
use futures::channel::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct PartitionForNamespace {
    nodes: Vec<(u32, Option<Arc<Node>>)>,
    replicas: usize,
}
type PartitionTable = HashMap<String, PartitionForNamespace>;

impl Default for PartitionForNamespace {
    fn default() -> Self {
        Self { nodes: Vec::default(), replicas: 0 }
    }
}

impl PartitionForNamespace {
    fn all_replicas(&self, index: usize) -> impl Iterator<Item = Option<Arc<Node>>> + '_ {
        (0..self.replicas).map(move |i|self.nodes.get(i * node::PARTITIONS + index).and_then(|(_, item)|item.clone()))
    }

    fn get_node(&self, cluster: &Cluster, partition: &Partition<'_>, replica: crate::policy::Replica, last_tried: Weak<Node>) -> Result<Arc<Node>> {
        fn get_next_in_sequence<I: Iterator<Item = Arc<Node>>, F: Fn()->I>(get_sequence: F, last_tried: Weak<Node>) -> Option<Arc<Node>> {
            if let Some(last_tried) = last_tried.upgrade() {
                // If this isn't the first attempt, try the replica immediately after in sequence (that is actually valid)
                let mut replicas = get_sequence();
                while let Some(replica) = replicas.next() {
                    if Arc::ptr_eq(&replica, &last_tried) {
                        if let Some(in_sequence_after) = replicas.next() {
                            return Some(in_sequence_after)
                        }

                        // No more after this? Drop through to try from the beginning.
                        break;
                    }
                }
            }
            // If we get here, we're on the first attempt, the last node is already gone, or there are no more nodes in sequence. Just find the next populated option.
            get_sequence().next()
        }


        let node = match replica {
            crate::policy::Replica::Master => self.all_replicas(partition.partition_id).next().flatten(),
            crate::policy::Replica::Sequence => {
                get_next_in_sequence(||self.all_replicas(partition.partition_id).flatten(), last_tried)
            },
            crate::policy::Replica::PreferRack => {
                let rack_ids = cluster.client_policy.rack_ids.as_ref().ok_or_else(||"Attempted to use Replica::PreferRack without configuring racks in client policy".to_string())?;
                get_next_in_sequence(||
                    self
                    .all_replicas(partition.partition_id)
                    .flatten()
                    .filter(|node|node.is_in_rack(partition.namespace, rack_ids)), last_tried.clone())
                .or_else(||get_next_in_sequence(||self.all_replicas(partition.partition_id).flatten(), last_tried))
            },
        };
        
        node.ok_or_else(||format!("Cannot get appropriate node for namespace: {} partition: {}", partition.namespace, partition.partition_id).into())
    }
}

// Cluster encapsulates the aerospike cluster nodes and manages
// them.
#[derive(Debug)]
pub struct Cluster {
    // Initial host nodes specified by user.
    seeds: Arc<Mutex<Vec<Host>>>,

    // All aliases for all nodes in cluster.
    aliases: Arc<Mutex<HashMap<Host, Arc<Node>>>>,

    // Active nodes in cluster.
    nodes: Arc<Mutex<Vec<Arc<Node>>>>,

    // Which partition contains the key.
    partition_write_map: Mutex<PartitionTable>,

    // Random node index.
    node_index: AtomicIsize,

    client_policy: ClientPolicy,

    tend_channel: Mutex<Sender<()>>,
    closed: AtomicBool,
}

impl Cluster {
    pub async fn new(policy: ClientPolicy, hosts: &[Host]) -> Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel(100);
        let cluster = Arc::new(Cluster {
            client_policy: policy,

            seeds: Arc::new(Mutex::new(hosts.to_vec())),
            aliases: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(vec![])),

            partition_write_map: Mutex::new(HashMap::default()),
            node_index: AtomicIsize::new(0),

            tend_channel: Mutex::new(tx),
            closed: AtomicBool::new(false),
        });
        // try to seed connections for first use
        Cluster::wait_till_stabilized(cluster.clone()).await?;

        // apply policy rules
        if cluster.client_policy.fail_if_not_connected && !cluster.is_connected() {
            bail!(ErrorKind::Connection(
                "Failed to connect to host(s). The network \
                 connection(s) to cluster nodes may have timed out, or \
                 the cluster may be in a state of flux."
                    .to_string()
            ));
        }

        let cluster_for_tend = cluster.clone();
        let _res = aerospike_rt::spawn(Cluster::tend_thread(cluster_for_tend, rx));
        debug!("New cluster initialized and ready to be used...");
        Ok(cluster)
    }

    async fn tend_thread(cluster: Arc<Cluster>, mut rx: Receiver<()>) {
        let tend_interval = cluster.client_policy.tend_interval;

        loop {
            if rx.try_next().is_ok() {
                unreachable!();
            } else if let Err(err) = cluster.tend().await {
                log_error_chain!(err, "Error tending cluster");
            }
            aerospike_rt::sleep(tend_interval).await;
        }

        // close all nodes
        //let nodes = cluster.nodes().await;
        //for mut node in nodes {
        //    if let Some(node) = Arc::get_mut(&mut node) {
        //        node.close().await;
        //    }
        //}
        //cluster.set_nodes(vec![]).await;
    }

    async fn tend(&self) -> Result<()> {
        let mut nodes = self.nodes();

        // All node additions/deletions are performed in tend thread.
        // If active nodes don't exist, seed cluster.
        if nodes.is_empty() {
            debug!("No connections available; seeding...");
            self.seed_nodes().await;
            nodes = self.nodes();
        }

        let mut friend_list: Vec<Host> = vec![];
        let mut refresh_count = 0;

        // Refresh all known nodes.
        for node in nodes {
            let old_gen = node.partition_generation();
            let old_rebalance_gen = node.rebalance_generation();
            if node.is_active() {
                match node.refresh(self.aliases()).await {
                    Ok(friends) => {
                        refresh_count += 1;

                        if !friends.is_empty() {
                            friend_list.extend_from_slice(&friends);
                        }

                        if old_gen != node.partition_generation() {
                            self.update_partitions(&node).await?;
                        }

                        if old_rebalance_gen != node.rebalance_generation() {
                            self.update_rack_ids(&node).await?;
                        }
                    }
                    Err(err) => {
                        node.increase_failures();
                        warn!("Node `{}` refresh failed: {}", node, err);
                    }
                }
            }
        }

        // Add nodes in a batch.
        let add_list = self.find_new_nodes_to_add(friend_list).await;
        self.add_nodes_and_aliases(&add_list);

        // IMPORTANT: Remove must come after add to remove aliases
        // Handle nodes changes determined from refreshes.
        // Remove nodes in a batch.
        let remove_list = self.find_nodes_to_remove(refresh_count).await;
        self.remove_nodes_and_aliases(remove_list).await;

        Ok(())
    }

    async fn wait_till_stabilized(cluster: Arc<Cluster>) -> Result<()> {
        let timeout = cluster
            .client_policy()
            .timeout
            .unwrap_or_else(|| Duration::from_secs(3));
        let deadline = Instant::now() + timeout;
        let sleep_between_tend = Duration::from_millis(1);

        let mut count: isize = -1;
        loop {
            if Instant::now() > deadline {
                break;
            }

            if let Err(err) = cluster.tend().await {
                log_error_chain!(err, "Error during initial cluster tend");
            }

            let old_count = count;
            count = cluster.nodes().len() as isize;
            if count == old_count {
                break;
            }

            aerospike_rt::sleep(sleep_between_tend).await;
        }
        
        Ok(())
    }

    pub const fn cluster_name(&self) -> &Option<String> {
        &self.client_policy.cluster_name
    }

    pub const fn client_policy(&self) -> &ClientPolicy {
        &self.client_policy
    }

    pub fn add_seeds(&self, new_seeds: &[Host]) -> Result<()> {
        self.seeds.lock().unwrap().extend_from_slice(new_seeds);

        Ok(())
    }

    pub fn alias_exists(&self, host: &Host) -> Result<bool> {
        let aliases = self.aliases.lock().unwrap();
        Ok(aliases.contains_key(host))
    }


    pub fn node_partitions(&self, node: &Node, namespace: &str) -> Vec<u16> {
        let mut res: Vec<u16> = vec![];
        let partitions = self.partition_write_map.lock().unwrap();

        if let Some(node_array) = partitions.get(namespace) {
            for (i, (_, tnode)) in node_array.nodes.iter().enumerate().take(node::PARTITIONS) {
                if tnode.as_ref().map_or(false, |tnode|tnode.as_ref() == node) {
                    res.push(i as u16);
                }
            }
        }

        res
    }

    pub async fn update_partitions(&self, node: &Arc<Node>) -> Result<()> {
        let mut conn = node.get_connection().await?;
        let tokens = PartitionTokenizer::new(&mut conn, node).await.map_err(|e| {
            conn.invalidate();
            e
        })?;

        let mut partitions = self.partition_write_map.lock().unwrap();
        tokens.update_partition(&mut partitions, node)?;

        Ok(())
    }

    pub async fn update_rack_ids(&self, node: &Arc<Node>) -> Result<()> {
        const RACK_IDS: &str = "rack-ids";
        let mut conn = node.get_connection().await?;
        let info_map = Message::info(&mut conn, &[RACK_IDS, node::REBALANCE_GENERATION]).await?;
        if let Some(buf) = info_map.get(RACK_IDS) {
            node.parse_rack(buf.as_str())?;
        }

        // We re-update the rebalance generation right now (in case its changed since it was last polled)
        node.update_rebalance_generation(&info_map)?;

        Ok(())
    }

    pub async fn seed_nodes(&self) -> bool {
        let seed_array = self.seeds.lock().unwrap().clone();

        info!("Seeding the cluster. Seeds count: {}", seed_array.len());

        let mut list: Vec<Arc<Node>> = vec![];
        for seed in &*seed_array {
            let mut seed_node_validator = NodeValidator::new(self);
            if let Err(err) = seed_node_validator.validate_node(self, seed).await {
                log_error_chain!(err, "Failed to validate seed host: {}", seed);
                continue;
            };

            for alias in &*seed_node_validator.aliases() {
                let nv = if *seed == *alias {
                    seed_node_validator.clone()
                } else {
                    let mut nv2 = NodeValidator::new(self);
                    if let Err(err) = nv2.validate_node(self, seed).await {
                        log_error_chain!(err, "Seeding host {} failed with error", alias);
                        continue;
                    };
                    nv2
                };

                if self.find_node_name(&list, &nv.name) {
                    continue;
                }

                let node = self.create_node(nv);
                let node = Arc::new(node);
                self.add_aliases(node.clone());
                list.push(node);
            }
        }

        self.add_nodes_and_aliases(&list);
        !list.is_empty()
    }

    fn find_node_name(&self, list: &[Arc<Node>], name: &str) -> bool {
        list.iter().any(|node| node.name() == name)
    }

    async fn find_new_nodes_to_add(&self, hosts: Vec<Host>) -> Vec<Arc<Node>> {
        let mut list: Vec<Arc<Node>> = vec![];

        for host in hosts {
            let mut nv = NodeValidator::new(self);
            if let Err(err) = nv.validate_node(self, &host).await {
                log_error_chain!(err, "Adding node {} failed with error", host.name);
                continue;
            };

            // Duplicate node name found. This usually occurs when the server
            // services list contains both internal and external IP addresses
            // for the same node. Add new host to list of alias filters
            // and do not add new node.
            let mut dup = false;
            match self.get_node_by_name(&nv.name) {
                Ok(node) => {
                    self.add_alias(host, node.clone());
                    dup = true;
                }
                Err(_) => {
                    if let Some(node) = list.iter().find(|n| n.name() == nv.name) {
                        self.add_alias(host, node.clone());
                        dup = true;
                    }
                }
            };

            if !dup {
                let node = self.create_node(nv);
                list.push(Arc::new(node));
            }
        }

        list
    }

    fn create_node(&self, nv: NodeValidator) -> Node {
        Node::new(self.client_policy.clone(), Arc::new(nv))
    }

    async fn find_nodes_to_remove(&self, refresh_count: usize) -> Vec<Arc<Node>> {
        let nodes = self.nodes();
        let mut remove_list: Vec<Arc<Node>> = vec![];
        let cluster_size = nodes.len();
        for node in nodes {
            let tnode = node.clone();

            if !node.is_active() {
                remove_list.push(tnode);
                continue;
            }

            match cluster_size {
                // Single node clusters rely on whether it responded to info requests.
                1 if node.failures() > 5 => {
                    // 5 consecutive info requests failed. Try seeds.
                    if self.seed_nodes().await {
                        remove_list.push(tnode);
                    }
                }

                // Two node clusters require at least one successful refresh before removing.
                2 if refresh_count == 1 && node.reference_count() == 0 && node.failures() > 0 => {
                    remove_list.push(node);
                }

                _ => {
                    // Multi-node clusters require two successful node refreshes before removing.
                    if refresh_count >= 2 && node.reference_count() == 0 {
                        // Node is not referenced by other nodes.
                        // Check if node responded to info request.
                        if node.failures() == 0 {
                            // Node is alive, but not referenced by other nodes.  Check if mapped.
                            if !self.find_node_in_partition_map(node) {
                                remove_list.push(tnode);
                            }
                        } else {
                            // Node not responding. Remove it.
                            remove_list.push(tnode);
                        }
                    }
                }
            }
        }

        remove_list
    }

    fn add_nodes_and_aliases(&self, friend_list: &[Arc<Node>]) {
        for node in friend_list {
            self.add_aliases(node.clone());
        }
        self.add_nodes(friend_list);
    }

    async fn remove_nodes_and_aliases(&self, mut nodes_to_remove: Vec<Arc<Node>>) {
        for node in &mut nodes_to_remove {
            for alias in node.aliases() {
                self.remove_alias(&alias);
            }
            if let Some(node) = Arc::get_mut(node) {
                node.close().await;
            }
        }
        self.remove_nodes(&nodes_to_remove);
    }

    fn add_alias(&self, host: Host, node: Arc<Node>) {
        let mut aliases = self.aliases.lock().unwrap();
        node.add_alias(host.clone());
        aliases.insert(host, node);
    }

    fn remove_alias(&self, host: &Host) {
        let mut aliases = self.aliases.lock().unwrap();
        aliases.remove(host);
    }

    fn add_aliases(&self, node: Arc<Node>) {
        let mut aliases = self.aliases.lock().unwrap();
        for alias in node.aliases() {
            aliases.insert(alias, node.clone());
        }
    }

    fn find_node_in_partition_map(&self, filter: Arc<Node>) -> bool {
        let filter = Some(filter);
        let partitions = self.partition_write_map.lock().unwrap();
        (*partitions)
            .values()
            .any(|map| map.nodes.iter().any(|(_, node)| *node == filter))
    }

    fn add_nodes(&self, friend_list: &[Arc<Node>]) {
        if friend_list.is_empty() {
            return;
        }

        let mut nodes = self.nodes();
        nodes.extend(friend_list.iter().cloned());
        self.set_nodes(nodes);
    }

    fn remove_nodes(&self, nodes_to_remove: &[Arc<Node>]) {
        if nodes_to_remove.is_empty() {
            return;
        }

        let nodes = self.nodes();
        let mut node_array: Vec<Arc<Node>> = vec![];

        for node in &nodes {
            if !nodes_to_remove.contains(node) {
                node_array.push(node.clone());
            }
        }

        self.set_nodes(node_array);
    }

    pub fn is_connected(&self) -> bool {
        let nodes = self.nodes();
        let closed = self.closed.load(Ordering::Relaxed);
        !nodes.is_empty() && !closed
    }

    pub fn aliases(&self) -> HashMap<Host, Arc<Node>> {
        self.aliases.lock().unwrap().clone()
    }

    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.nodes.lock().unwrap().clone()
    }

    fn set_nodes(&self, new_nodes: Vec<Arc<Node>>) {
        let mut nodes = self.nodes.lock().unwrap();
        *nodes = new_nodes;
    }

    pub fn get_node(&self, partition: &Partition<'_>, replica: crate::policy::Replica, last_tried: Weak<Node>) -> Result<Arc<Node>> {
        let partitions = self.partition_write_map.lock().unwrap();

        let namespace = partitions
            .get(partition.namespace)
            .ok_or_else(||format!("Cannot get appropriate node for namespace: {}", partition.namespace))?;
    
        namespace.get_node(self, partition, replica, last_tried)
    }

    pub fn get_random_node(&self) -> Result<Arc<Node>> {
        let node_array = self.nodes();
        let length = node_array.len() as isize;

        for _ in 0..length {
            let index = ((self.node_index.fetch_add(1, Ordering::Relaxed) + 1) % length).abs();
            if let Some(node) = node_array.get(index as usize) {
                if node.is_active() {
                    return Ok(node.clone());
                }
            }
        }

        bail!("No active node")
    }

    pub fn get_node_by_name(&self, node_name: &str) -> Result<Arc<Node>> {
        let node_array = self.nodes();

        for node in &node_array {
            if node.name() == node_name {
                return Ok(node.clone());
            }
        }

        bail!("Requested node `{}` not found.", node_name)
    }

    pub fn close(&self) -> Result<()> {
        if !self.closed.load(Ordering::Relaxed) {
            // close tend by closing the channel
            let tx = self.tend_channel.lock().unwrap();
            drop(tx);
            self.closed.store(true, Ordering::Relaxed);
        }

        Ok(())
    }
}
