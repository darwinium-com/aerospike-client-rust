#![allow(dead_code)]
#![allow(unused_imports)]
// #![allow(non_camel_case_types)]

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate core;
extern crate byteorder;
extern crate crypto;
extern crate bytebuffer;
extern crate rustc_serialize;
extern crate crossbeam;
extern crate rand;
extern crate threadpool;
extern crate pwhash;

use command::info_command::*;
use msgpack::encoder::*;

pub use value::Value;
pub use policy::{Policy, ClientPolicy, ReadPolicy, WritePolicy, Priority, ConsistencyLevel,
                 CommitLevel, RecordExistsAction, GenerationPolicy, ScanPolicy, QueryPolicy};
pub use net::{Host, Connection};
pub use cluster::{Node, Cluster};
pub use error::{AerospikeError, ResultCode, AerospikeResult};
pub use client::Client;
pub use common::{Key, Bin, Operation, UDFLang, Recordset, Statement, Filter, IndexType,
                 CollectionIndexType, ParticleType};

mod command;
mod msgpack;

pub mod common;
pub mod value;
pub mod policy;
pub mod net;
pub mod cluster;
pub mod error;
pub mod client;

use internal::wait_group::WaitGroup;

mod internal;