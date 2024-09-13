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

pub mod metadata_ops;

use metadata_ops::RpcResult;
use tokio;
use tonic::transport::Server;

use crate::metadata_ops::{
    metadata_ops_server::{MetadataOps, MetadataOpsServer},
    *,
};

#[derive(Debug, Default)]
struct MetadataOpsImpl {}

#[tonic::async_trait]
impl MetadataOps for MetadataOpsImpl {
    async fn lookup(
        &self,
        request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<LookupResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn forget(
        &self,
        request: tonic::Request<ForgetRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn get_attr(
        &self,
        request: tonic::Request<GlobalNodeId>,
    ) -> std::result::Result<tonic::Response<GetAttrResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn set_attr(
        &self,
        request: tonic::Request<SetAttrRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn read_link(
        &self,
        request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<ReadLinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn make_node(
        &self,
        request: tonic::Request<MakeNodeRequest>,
    ) -> std::result::Result<tonic::Response<MakeNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn make_dir(
        &self,
        request: tonic::Request<MakeNodeRequest>,
    ) -> std::result::Result<tonic::Response<MakeNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn unlink(
        &self,
        request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn remove_dir(
        &self,
        request: tonic::Request<GlobalDirEntry>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn symlink(
        &self,
        request: tonic::Request<SymlinkRequest>,
    ) -> std::result::Result<tonic::Response<SymlinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn rename(
        &self,
        request: tonic::Request<RenameRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn link(
        &self,
        request: tonic::Request<LinkRequest>,
    ) -> std::result::Result<tonic::Response<LinkResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn open_dir(
        &self,
        request: tonic::Request<GlobalNodeId>,
    ) -> std::result::Result<tonic::Response<OpenNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn read_dir(
        &self,
        request: tonic::Request<ReadDirRequest>,
    ) -> std::result::Result<tonic::Response<ReadDirResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn release_dir(
        &self,
        request: tonic::Request<ReleaseDirRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn read_dir_plus(
        &self,
        request: tonic::Request<ReadDirRequest>,
    ) -> std::result::Result<tonic::Response<ReadDirResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn open(
        &self,
        request: tonic::Request<OpenRequest>,
    ) -> std::result::Result<tonic::Response<OpenNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    async fn close(
        &self,
        request: tonic::Request<CloseRequest>,
    ) -> std::result::Result<tonic::Response<RpcResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();

    let metadata_ops = MetadataOpsImpl::default();

    Server::builder()
        .add_service(MetadataOpsServer::new(metadata_ops))
        .serve(addr)
        .await?;

    Ok(())
}
