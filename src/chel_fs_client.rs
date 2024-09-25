/*
 *  Copyright (C) 2024 github.com/chel-data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

mod file_utils;
mod metadata_ops;

use file_utils::{apply_umask, get_file_perm, get_file_type, FILE_PERM_DEF_REG, FILE_TYPE_REG};
use fuser::consts::{FOPEN_DIRECT_IO, FOPEN_NONSEEKABLE};
use fuser::{
    FileAttr, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use libc::{EFAULT, ENOENT};
use metadata_ops::{
    metadata_ops_client::MetadataOpsClient, Attrs, DirEntry, DirEntryInfo, GetAttrResponse,
    GlobalDirEntry, NodeId, NodeInfo, OpenNodeResponse, ReadDirRequest, ReleaseDirRequest,
};
use metadata_ops::{GlobalNodeId, MakeNodeRequest, MakeNodeResponse, OpenHandle, ReadDirResponse};
use rand::RngCore;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::hash::{DefaultHasher, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::ops::Add;
use std::os::unix::ffi::OsStrExt;
use std::sync::{atomic::AtomicU64, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use tokio::runtime::Builder;
use tonic::transport::Channel;

const ROOT_INODE_NUMBER: u64 = 1;

pub struct ChelFs2Fuse {
    client: MetadataOpsClient<tonic::transport::Channel>,
    async_runtime: tokio::runtime::Runtime,
    pool_id: String,
    cont_id: String,
    id_map: RwLock<HashMap<u64, (NodeId, Vec<u8>, NodeId)>>,
    generation: AtomicU64,
}

impl From<Attrs> for FileAttr {
    fn from(attrs: Attrs) -> Self {
        let mode = attrs.mode.unwrap_or(FILE_TYPE_REG | FILE_PERM_DEF_REG);
        FileAttr {
            ino: 0,
            size: attrs.size.unwrap_or(0),
            blocks: 1,
            atime: UNIX_EPOCH.add(Duration::from_secs(attrs.atime.unwrap_or(0))),
            mtime: UNIX_EPOCH.add(Duration::from_secs(attrs.mtime.unwrap_or(0))),
            ctime: UNIX_EPOCH.add(Duration::from_secs(attrs.ctime.unwrap_or(0))),
            crtime: UNIX_EPOCH.add(Duration::from_secs(attrs.ctime.unwrap_or(0))),
            kind: get_file_type(mode),
            perm: get_file_perm(mode) as u16,
            nlink: attrs.nlink.unwrap_or(1),
            uid: attrs.uid.unwrap_or(0),
            gid: attrs.gid.unwrap_or(0),
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }
}

impl ChelFs2Fuse {
    pub fn new(
        client: MetadataOpsClient<tonic::transport::Channel>,
        async_runtime: tokio::runtime::Runtime,
    ) -> Self {
        ChelFs2Fuse {
            client,
            async_runtime,
            pool_id: "pool1".to_string(),
            cont_id: "cont1".to_string(),
            id_map: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(rand::thread_rng().next_u64()),
        }
    }

    fn find_node_id(&self, ino: u64) -> Result<NodeId> {
        if ino == ROOT_INODE_NUMBER {
            return Ok(NodeId { hi: 0, lo: 0 });
        }

        let read_map = self.id_map.read().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to acquire read lock, error: {}", e),
            )
        })?;

        match read_map.get(&ino) {
            Some(node_id) => Ok(node_id.2.clone()),
            None => Err(Error::new(
                ErrorKind::Other,
                format!("node id not found for ino: {}", ino),
            )),
        }
    }

    fn find_parent(&self, ino: u64) -> Result<(NodeId, Vec<u8>)> {
        let read_map = self.id_map.read().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to acquire read lock, error: {}", e),
            )
        })?;

        match read_map.get(&ino) {
            Some(node_id) => Ok((node_id.0.clone(), node_id.1.clone())),
            None => Err(Error::new(
                ErrorKind::Other,
                format!("node id not found for ino: {}", ino),
            )),
        }
    }

    fn insert_id_map(
        &self,
        ino: u64,
        parent_id: NodeId,
        name: Vec<u8>,
        node_id: NodeId,
    ) -> Result<()> {
        let mut write_map = self.id_map.write().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to acquire write lock, error: {}", e),
            )
        })?;

        if write_map.get(&ino).is_none() {
            write_map.insert(ino, (parent_id, name, node_id));
        }

        Ok(())
    }

    fn generate_inum(node_id: NodeId) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(node_id.lo);
        hasher.write_u64(node_id.hi);
        hasher.finish()
    }

    fn gen_generation(&self) -> u64 {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn call_get_attr(&mut self, parent_id: NodeId, name: Vec<u8>) -> Result<GetAttrResponse> {
        let request = tonic::Request::new(GlobalDirEntry {
            pool_id: self.pool_id.clone(),
            cont_id: self.cont_id.clone(),
            entry: DirEntry {
                parent: parent_id,
                name,
            },
        });

        self.async_runtime.block_on(async {
            let res = self.client.get_attr(request).await;
            match res {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("call get_attr failed, status: {}", e),
                )),
            }
        })
    }

    fn make_file_attr(ino: u64, attrs: Attrs) -> FileAttr {
        let mut file_attr: FileAttr = attrs.into();
        file_attr.ino = ino;
        file_attr
    }
}

impl Filesystem for ChelFs2Fuse {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_id = self.find_node_id(parent).unwrap();

        let res = self.call_get_attr(parent_id, name.as_bytes().to_vec());
        if res.is_err() {
            eprintln!(
                "call get_attr failed, msg: {}",
                res.unwrap_err().to_string()
            );
            reply.error(ENOENT);
            return;
        }

        match res.unwrap() {
            GetAttrResponse {
                res,
                node_info: None,
            } => {
                eprintln!(
                    "error get_attr response, code: {}, reason: {}",
                    res.code,
                    res.reason.unwrap_or("empty".to_string())
                );
                reply.error(EFAULT);
                return;
            }
            GetAttrResponse {
                res: _,
                node_info: Some(info),
            } => {
                let NodeInfo {
                    node: node_id,
                    attrs,
                } = info;
                let ino = ChelFs2Fuse::generate_inum(node_id);
                let res = self.insert_id_map(ino, parent_id, name.as_bytes().to_vec(), node_id);
                if res.is_err() {
                    eprintln!(
                        "insert id map failed, error: {}",
                        res.unwrap_err().to_string()
                    );
                    reply.error(EFAULT);
                    return;
                }

                let attr = Self::make_file_attr(ino, attrs);
                reply.entry(&Duration::ZERO, &attr, self.gen_generation());
                return;
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let (parent_id, name) = if ino == ROOT_INODE_NUMBER {
            (NodeId { hi: 0, lo: 0 }, vec![b'.'])
        } else {
            let res = self.find_parent(ino);
            if res.is_err() {
                eprintln!(
                    "find parent failed, error: {}",
                    res.unwrap_err().to_string()
                );
                reply.error(ENOENT);
                return;
            }
            res.unwrap()
        };

        let res = self.call_get_attr(parent_id, name);
        if res.is_err() {
            eprintln!(
                "call get_attr failed, error: {}",
                res.unwrap_err().to_string()
            );
            reply.error(EFAULT);
            return;
        }

        match res.unwrap() {
            GetAttrResponse {
                res,
                node_info: None,
            } => {
                eprintln!(
                    "error get_attr response, code: {}, reason: {}",
                    res.code,
                    res.reason.unwrap_or("empty".to_string())
                );
                reply.error(ENOENT);
                return;
            }
            GetAttrResponse {
                res: _,
                node_info: Some(info),
            } => {
                let NodeInfo {
                    node: _,
                    attrs,
                } = info;
                let attr = Self::make_file_attr(ino, attrs);
                reply.attr(&Duration::ZERO, &attr);
                return;
            }
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let parent_id = self.find_node_id(parent).unwrap();

        let request = tonic::Request::new(MakeNodeRequest {
            pool_id: self.pool_id.clone(),
            cont_id: self.cont_id.clone(),
            node: DirEntry {
                parent: parent_id,
                name: name.as_bytes().to_vec(),
            },
            mode: apply_umask(mode, umask),
        });

        let res = self.async_runtime.block_on(async {
            let res = self.client.make_node(request).await;
            match res {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("call make_node failed, status: {}", e),
                )),
            }
        });

        if res.is_err() {
            eprintln!(
                "call make_node failed, msg: {}",
                res.unwrap_err().to_string()
            );
            reply.error(EFAULT);
            return;
        }

        match res.unwrap() {
            MakeNodeResponse {
                res,
                node_info: None,
            } => {
                eprintln!(
                    "make_node return error, code: {}, reason: {}",
                    res.code,
                    res.reason.unwrap_or("unknown".to_string())
                );
                reply.error(EFAULT);
                return;
            }
            MakeNodeResponse {
                res: _,
                node_info: Some(info),
            } => {
                let NodeInfo {
                    node: node_id,
                    attrs,
                } = info;

                let ino = ChelFs2Fuse::generate_inum(node_id);
                let res = self.insert_id_map(ino, parent_id, name.as_bytes().to_vec(), node_id);
                if res.is_err() {
                    eprintln!(
                        "insert id map failed, error: {}",
                        res.unwrap_err().to_string()
                    );
                    reply.error(EFAULT);
                    return;
                }

                let attr = Self::make_file_attr(ino, attrs);
                reply.entry(&Duration::ZERO, &attr, self.gen_generation());
                return;
            }
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let res = self.find_node_id(ino);
        if res.is_err() {
            eprintln!("can't find node id for ino: {}", ino);
            reply.error(ENOENT);
            return;
        }

        let request = tonic::Request::new(GlobalNodeId {
            pool_id: self.pool_id.clone(),
            cont_id: self.cont_id.clone(),
            node: res.unwrap(),
        });

        let res = self.async_runtime.block_on(async {
            let res = self.client.open_dir(request).await;
            match res {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("call open_dir failed, status: {}", e),
                )),
            }
        });

        if res.is_err() {
            eprintln!(
                "call open_dir failed, msg: {}",
                res.unwrap_err().to_string()
            );
            reply.error(ENOENT);
            return;
        }

        match res.unwrap() {
            OpenNodeResponse {
                res,
                handle: None,
            } => {
                eprintln!(
                    "open_node return error, code: {}, reason: {}",
                    res.code,
                    res.reason.unwrap_or("unknown".to_string())
                );
                reply.error(EFAULT);
                return;
            }
            OpenNodeResponse {
                res: _,
                handle: Some(handle),
            } => {
                // lo is a non repeating u64
                reply.opened(handle.lo, 0);
                return;
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let parent_id = self.find_node_id(ino);
        if parent_id.is_err() {
            eprintln!("can't find node id for ino: {}", ino);
            reply.error(ENOENT);
            return;
        }

        let parent_id = parent_id.unwrap();
        let request = tonic::Request::new(ReadDirRequest {
            pool_id: self.pool_id.clone(),
            cont_id: self.cont_id.clone(),
            dir: parent_id.clone(),
            handle: OpenHandle { lo: fh, hi: 0 },
            offset: offset,
        });

        let res = self.async_runtime.block_on(async {
            let res = self.client.read_dir(request).await;
            match res {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("call read_dir failed, status: {}", e),
                )),
            }
        });

        if res.is_err() {
            eprintln!(
                "call read_dir failed, msg: {}",
                res.unwrap_err().to_string()
            );
            reply.error(EFAULT);
            return;
        }

        match res.unwrap() {
            ReadDirResponse {
                res,
                entries: None,
            } => {
                eprintln!(
                    "read_dir return error, code: {}, reason: {}",
                    res.code,
                    res.reason.unwrap_or("unknown".to_string())
                );
                reply.error(EFAULT);
                return;
            }
            ReadDirResponse {
                res: _,
                entries: Some(entry_set),
            } => {
                let mut entry_offset = offset;
                for entry in entry_set.entries {
                    let DirEntryInfo { name, node } = entry;
                    if node.is_none() {
                        eprintln!("node info is missing");
                        reply.error(EFAULT);
                        return;
                    }

                    let NodeInfo {
                        node: node_id,
                        attrs,
                    } = node.unwrap();
                    let ino = ChelFs2Fuse::generate_inum(node_id);
                    let file_type = get_file_type(attrs.mode.unwrap_or(FILE_TYPE_REG));
                    let entry_name = OsStr::from_bytes(&name).to_owned();
                    self.insert_id_map(ino, parent_id, name, node_id)
                        .expect("insert id map failed");
                    if reply.add(ino, entry_offset, file_type, &entry_name) {
                        break;
                    }

                    entry_offset += 1;
                }
                reply.ok();
                return;
            }
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        let parent_id = self.find_node_id(ino);
        if parent_id.is_err() {
            eprintln!("can't find node id for ino: {}", ino);
            reply.error(ENOENT);
            return;
        }

        let parent_id = parent_id.unwrap();
        let request = tonic::Request::new(ReleaseDirRequest {
            pool_id: self.pool_id.clone(),
            cont_id: self.cont_id.clone(),
            dir: parent_id.clone(),
            handle: OpenHandle { lo: fh, hi: 0 },
        });

        let res = self.async_runtime.block_on(async {
            let res = self.client.release_dir(request).await;
            match res {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("call release_dir failed, status: {}", e),
                )),
            }
        });

        if res.is_err() {
            eprintln!(
                "call release_dir failed, msg: {}",
                res.unwrap_err().to_string()
            );
            reply.error(EFAULT);
            return;
        }

        let result = res.unwrap();
        if result.code != 0 {
            eprintln!(
                "close_dir return error, code: {}, reason: {}",
                result.code,
                result.reason.unwrap_or("unknown".to_string())
            );
            reply.error(EFAULT);
            return;
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(ino, FOPEN_DIRECT_IO | FOPEN_NONSEEKABLE);
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        _offset: i64,
        _size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let str = format!("ino = {}", ino);
        reply.data(str.as_ref());
    }
}

fn main() {
    let async_runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let client = async_runtime
        .block_on(async {
            let client = MetadataOpsClient::connect("http://[::1]:50051").await?;
            Ok::<MetadataOpsClient<Channel>, tonic::transport::Error>(client)
        })
        .unwrap();

    let fs = ChelFs2Fuse::new(client, async_runtime);

    let options = vec![
        MountOption::RW,
        MountOption::FSName("chel_fs_fuse".to_string()),
    ];
    fuser::mount2(fs, "/mnt/fs2", &options).expect("mount error");
}
