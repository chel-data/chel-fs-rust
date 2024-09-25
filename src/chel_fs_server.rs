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

//mod file_utils;
mod metadata_ops;
mod metadata_store;
use daos_rust_api::daos_cont::DaosContainer;
use daos_rust_api::daos_pool::{DaosObjectId, DaosPool};
use daos_rust_api::daos_oid_allocator::DaosAsyncOidAllocator;
use metadata_ops::{
    metadata_ops_server::{MetadataOps, MetadataOpsServer},
    *,
};
use metadata_store::{Inode, MetadataStore};
use std::io::Result;
use std::sync::Arc;
use tokio;
use tokio::task;
use tonic::transport::Server;

const DEFAULT_POOL_NAME: &str = "pool1";
const DEFAULT_CONT_NAME: &str = "cont1";

impl From<Inode> for Attrs {
    fn from(inode: Inode) -> Self {
        Attrs {
            mode: Some(inode.mode),
            uid: Some(inode.uid),
            gid: Some(inode.gid),
            size: Some(inode.total_size),
            atime: Some(inode.atime),
            atime_nano: Some(inode.atime_nano),
            mtime: Some(inode.mtime),
            mtime_nano: Some(inode.mtime_nano),
            ctime: Some(inode.ctime),
            ctime_nano: Some(inode.ctime_nano),
            nlink: Some(inode.nlink),
        }
    }
}

impl RpcResult {
    pub fn ok() -> Self {
        RpcResult {
            code: 0,
            reason: None,
        }
    }
    pub fn err(reason: String) -> Self {
        RpcResult {
            code: 1,
            reason: Some(reason),
        }
    }
}

impl From<NodeId> for DaosObjectId {
    fn from(node_id: NodeId) -> Self {
        DaosObjectId {
            lo: node_id.lo,
            hi: node_id.hi,
        }
    }
}

#[derive(Debug)]
struct MetadataOpsImpl {
    store: Arc<MetadataStore>,
}

impl MetadataOpsImpl {
    pub fn new() -> Result<Self> {
        let mut pool = Box::new(DaosPool::new(DEFAULT_POOL_NAME));
        pool.connect()?;

        let mut cont = Box::new(DaosContainer::new(DEFAULT_CONT_NAME));
        cont.connect(pool.as_ref())?;

        let cont: Arc<DaosContainer> = Arc::from(cont);

        let allocator = DaosAsyncOidAllocator::new(cont.clone())?;

        Ok(MetadataOpsImpl {
            store: Arc::new(MetadataStore::new(Arc::from(pool), cont, Arc::from(allocator))),
        })
    }
}

#[tonic::async_trait]
impl MetadataOps for MetadataOpsImpl {
    async fn get_attr(
        &self,
        request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<GetAttrResponse>, tonic::Status> {
        let GlobalDirEntry {
            pool_id: _pool_id,
            cont_id: _cont_id,
            entry,
        } = request.into_inner();

        let DirEntry {
            parent,
            name,
        } = entry;

        let parent_oid = parent.into();

        let entry_inode = self.store.get_node(parent_oid, name).await;
        match entry_inode {
            Ok(inode) => Ok(tonic::Response::new(GetAttrResponse {
                res: RpcResult::ok(),
                node_info: Some(NodeInfo {
                    node: NodeId {
                        lo: inode.oid_lo,
                        hi: inode.oid_hi,
                    },
                    attrs: inode.into(),
                }),
            })),
            Err(e) => Ok(tonic::Response::new(GetAttrResponse {
                res: RpcResult::err(e.to_string()),
                node_info: None,
            })),
        }
    }

    async fn set_attr(
        &self,
        _request: tonic::Request<SetAttrRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn read_link(
        &self,
        _request: tonic::Request<GlobalNodeId>,
    ) -> std::result::Result<tonic::Response<ReadLinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn make_node(
        &self,
        request: tonic::Request<MakeNodeRequest>,
    ) -> std::result::Result<tonic::Response<MakeNodeResponse>, tonic::Status> {
        let MakeNodeRequest {
            pool_id: _pool_id,
            cont_id: _cont_id,
            node,
            mode,
        } = request.into_inner();

        let DirEntry {
            parent,
            name,
        } = node;

        let res = self
            .store
            .make_node(parent.into(), name, mode)
            .await;
        match res {
            Ok(inode) => Ok(tonic::Response::new(MakeNodeResponse {
                res: RpcResult::ok(),
                node_info: Some(NodeInfo {
                    node: NodeId {
                        lo: inode.oid_lo,
                        hi: inode.oid_hi,
                    },
                    attrs: inode.into(),
                }),
            })),
            Err(e) => Ok(tonic::Response::new(MakeNodeResponse {
                res: RpcResult::err(e.to_string()),
                node_info: None,
            })),
        }
    }

    async fn unlink(
        &self,
        _request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn remove_dir(
        &self,
        _request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn symlink(
        &self,
        _request: tonic::Request<SymlinkRequest>,
    ) -> std::result::Result<tonic::Response<SymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn rename(
        &self,
        _request: tonic::Request<RenameRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn link(
        &self,
        _request: tonic::Request<LinkRequest>,
    ) -> std::result::Result<tonic::Response<LinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn open_dir(
        &self,
        request: tonic::Request<GlobalNodeId>,
    ) -> std::result::Result<tonic::Response<OpenNodeResponse>, tonic::Status> {
        let GlobalNodeId {
            pool_id: _pool_id,
            cont_id: _cont_id,
            node,
        } = request.into_inner();

        let node_id = node.into();
        let res = self.store.open_dir(node_id).await;
        match res {
            Ok((lo, hi)) => Ok(tonic::Response::new(OpenNodeResponse {
                res: RpcResult::ok(),
                handle: Some(OpenHandle { lo, hi }),
            })),
            Err(e) => Ok(tonic::Response::new(OpenNodeResponse {
                res: RpcResult::err(e.to_string()),
                handle: None,
            })),
        }
    }

    async fn read_dir(
        &self,
        request: tonic::Request<ReadDirRequest>,
    ) -> std::result::Result<tonic::Response<ReadDirResponse>, tonic::Status> {
        let ReadDirRequest {
            pool_id: _pool_id,
            cont_id: _cont_id,
            dir: _dir,
            handle,
            offset,
        } = request.into_inner();

        const INFO_SET_DEFAULT_CAPACITY: usize = 64;
        let mut info_set = DirEntryInfoSet {
            entries: Vec::with_capacity(INFO_SET_DEFAULT_CAPACITY),
        };

        let res = self
            .store
            .read_dir((handle.lo, handle.hi), offset, |name: Vec<u8>, inode: Option<Inode>| {
                let entry = DirEntryInfo {
                    name,
                    node: inode.map(|inode| NodeInfo {
                        node: NodeId {
                            lo: inode.oid_lo,
                            hi: inode.oid_hi,
                        },
                        attrs: inode.into(),
                    }),
                };

                info_set.entries.push(entry);

                Ok(())
            })
            .await;

        match res {
            Ok(_) => Ok(tonic::Response::new(ReadDirResponse {
                res: RpcResult::ok(),
                entries: Some(info_set),
            })),
            Err(e) => Ok(tonic::Response::new(ReadDirResponse {
                res: RpcResult::err(e.to_string()),
                entries: None,
            })),
        }
    }

    async fn release_dir(
        &self,
        _request: tonic::Request<ReleaseDirRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        let ReleaseDirRequest {
            pool_id: _pool_id,
            cont_id: _cont_id,
            dir: _dir,
            handle,
        } = _request.into_inner();

        let res = self.store.close_dir((handle.lo, handle.hi)).await;
        match res {
            Ok(_) => Ok(tonic::Response::new(RpcResult::ok())),
            Err(e) => Ok(tonic::Response::new(RpcResult::err(e.to_string()))),
        }
    }

    async fn read_dir_plus(
        &self,
        _request: tonic::Request<ReadDirRequest>,
    ) -> std::result::Result<tonic::Response<ReadDirResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn open(
        &self,
        _request: tonic::Request<OpenRequest>,
    ) -> std::result::Result<tonic::Response<OpenNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn close(
        &self,
        _request: tonic::Request<CloseRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();

    let handle = task::spawn_blocking(|| MetadataOpsImpl::new());
    let metadata_ops = handle.await??;

    Server::builder()
        .add_service(MetadataOpsServer::new(metadata_ops))
        .serve(addr)
        .await?;

    Ok(())
}
