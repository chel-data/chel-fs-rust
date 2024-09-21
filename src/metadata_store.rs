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

use daos_rust_api::daos_cont::{DaosContainer, DaosContainerAsyncOps};
use daos_rust_api::daos_obj::{
    DaosObjAsyncOps, DaosObject, DAOS_COND_DKEY_INSERT, DAOS_OC_HINTS_NONE, DAOS_OC_UNKNOWN,
    DAOS_OT_ARRAY_BYTE,
};
use daos_rust_api::daos_oid_allocator::DaosAsyncOidAllocator;
use daos_rust_api::daos_pool::{DaosObjectId, DaosPool};
use daos_rust_api::daos_txn::{DaosTxn, DaosTxnAsyncOps};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec::Vec;
use tokio::sync::RwLock;
use zvariant::{serialized::Context, serialized::Data, to_bytes, Type, LE};

const OBJECT_ID_NIL: DaosObjectId = DaosObjectId { lo: 0, hi: 0 };
const DEFAULT_CHUNK_SIZE: u64 = 1 << 16;

#[derive(Deserialize, Serialize, Type, Debug)]
pub struct Inode {
    pub mode: u32,
    pub oid_lo: u64,
    pub oid_hi: u64,
    pub atime: u64,
    pub atime_nano: u64,
    pub mtime: u64,
    pub mtime_nano: u64,
    pub ctime: u64,
    pub ctime_nano: u64,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u64,
    pub chunk_size: u64,
    pub total_size: u64,
}

fn encode_inode(inode: &Inode) -> zvariant::Result<Data<'static, 'static>> {
    let ctxt = Context::new_dbus(LE, 0);
    to_bytes(ctxt, inode)
}

fn decode_inode(bytes: &[u8]) -> zvariant::Result<(Inode, usize)> {
    let ctxt = Context::new_dbus(LE, 0);
    let raw_data = Data::new(bytes, ctxt);
    raw_data.deserialize()
}

#[derive(Debug)]
pub struct MetadataStore {
    root: RwLock<Option<Arc<DaosObject>>>,
    pool: Arc<DaosPool>,
    cont: Arc<DaosContainer>,
    oid_allocator: Arc<DaosAsyncOidAllocator>,
    dir_cache: RwLock<HashMap<DaosObjectId, Arc<DaosObject>>>,
}

impl MetadataStore {
    pub fn new(
        pool: Arc<DaosPool>,
        cont: Arc<DaosContainer>,
        allocator: Arc<DaosAsyncOidAllocator>,
    ) -> Self {
        Self {
            root: RwLock::new(None),
            pool,
            cont,
            oid_allocator: allocator,
            dir_cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_node(&self, parent: DaosObjectId, entry_name: Vec<u8>) -> Result<Inode> {
        let parent_obj = if parent == OBJECT_ID_NIL {
            self.get_root().await
        } else {
            self.get_dir_obj(parent, false).await
        }?;

        let txn = DaosTxn::txn_none();
        let flags = 0;
        let akey = vec![0u8];

        let ino_buf = parent_obj
            .fetch_async(&txn, flags, entry_name, akey, 512)
            .await?;

        if let Ok((inode, length)) = decode_inode(ino_buf.as_slice()) {
            Ok(inode)
        } else {
            Err(Error::new(ErrorKind::InvalidData, "Failed to decode inode"))
        }
    }

    pub async fn make_node(&self, parent: DaosObjectId, name: Vec<u8>, mode: u32) -> Result<Inode> {
        let parent_obj = if parent == OBJECT_ID_NIL {
            self.get_root().await
        } else {
            self.get_dir_obj(parent, false).await
        }?;

        // This object would be leaked if any following step would fail
        let child_obj = DaosObject::create_async(
            self.get_container().as_ref(),
            self.oid_allocator.clone(),
            DAOS_OT_ARRAY_BYTE,
            DAOS_OC_UNKNOWN,
            DAOS_OC_HINTS_NONE,
            0,
        )
        .await?;

        let oid = child_obj.oid;

        let duration = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d,
            Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string())),
        };

        let inode = Inode {
            mode,
            oid_lo: oid.lo,
            oid_hi: oid.hi,
            atime: duration.as_secs(),
            atime_nano: duration.subsec_nanos() as u64,
            mtime: duration.as_secs(),
            mtime_nano: duration.subsec_nanos() as u64,
            ctime: duration.as_secs(),
            ctime_nano: duration.subsec_nanos() as u64,
            uid: 0,
            gid: 0,
            nlink: 1,
            chunk_size: DEFAULT_CHUNK_SIZE,
            total_size: 0,
        };

        let ino_buf = match encode_inode(&inode) {
            Ok(buf) => buf,
            Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string())),
        };

        let txn = DaosTxn::txn_none();
        let akey = vec![0u8];
        parent_obj
            .update_async(
                &txn,
                DAOS_COND_DKEY_INSERT as u64,
                name,
                akey,
                ino_buf.to_vec(),
            )
            .await?;

        Ok(inode)
    }

    pub async fn open_dir(&self, dir: DaosObjectId) -> Result<()> {
        let _ = self.get_dir_obj(dir, true).await?;
        Ok(())
    }

    async fn get_dir_obj(&self, dir: DaosObjectId, cache_in: bool) -> Result<Arc<DaosObject>> {
        let dir_cache = self.dir_cache.read().await;
        match dir_cache.get(&dir) {
            Some(dir_obj) => Ok(dir_obj.clone()),
            None => {
                drop(dir_cache);

                let daos_obj = DaosObject::open_async(self.cont.as_ref(), dir, false).await?;
                let dir_obj: Arc<DaosObject> = Arc::from(daos_obj);

                if cache_in {
                    let mut dir_cache = self.dir_cache.write().await;
                    dir_cache.insert(dir, dir_obj.clone());
                }

                Ok(dir_obj)
            }
        }
    }

    async fn get_root(&self) -> Result<Arc<DaosObject>> {
        let root_opt = self.root.read().await;
        match *root_opt {
            Some(ref root) => return Ok(root.clone()),
            None => {}
        }
        drop(root_opt);

        // make sure only one thread is creating the root object
        let mut root_opt = self.root.write().await;
        match *root_opt {
            Some(ref root) => return Ok(root.clone()),
            None => {}
        }

        let prop = self.cont.query_prop_async().await?;
        let co_roots = prop.get_co_roots()?;

        let root_oid = co_roots[1];

        let root_obj = DaosObject::open_async(self.cont.as_ref(), root_oid, true).await;
        match root_obj {
            Ok(obj) => {
                let root_arc: Arc<DaosObject> = Arc::from(obj);
                (*root_opt).replace(root_arc.clone());
                Ok(root_arc)
            }
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        }
    }

    fn get_pool(&self) -> Arc<DaosPool> {
        self.pool.clone()
    }

    fn get_container(&self) -> Arc<DaosContainer> {
        self.cont.clone()
    }
}
