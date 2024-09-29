/*
 *  Copyright (C) 2024 github./chel-data
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

use crate::file_utils::{make_mode, DEF_FILE_PERM_DIR, FILE_TYPE_DIR, DEFAULT_BLOCK_SIZE};
use daos_rust_api::daos_cont::{DaosContainer, DaosContainerAsyncOps};
use daos_rust_api::daos_obj::{
    DaosKeyList, DaosObjAsyncOps, DaosObject, DAOS_COND_DKEY_FETCH, DAOS_COND_DKEY_INSERT,
    DAOS_OC_HINTS_NONE, DAOS_OC_UNKNOWN, DAOS_OT_ARRAY_BYTE,
};
use daos_rust_api::daos_oid_allocator::DaosAsyncOidAllocator;
use daos_rust_api::daos_pool::{DaosObjectId, DaosPool};
use daos_rust_api::daos_txn::DaosTxn;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::ops::Range;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec::Vec;
use tokio::sync::RwLock;
use zvariant::{serialized::Context, serialized::Data, to_bytes, Type, LE};

const OBJECT_ID_NIL: DaosObjectId = DaosObjectId { lo: 0, hi: 0 };

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
    pub nlink: u32,
    pub chunk_size: u32,
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
struct OpenDirState {
    dir: Arc<DaosObject>,
    key_list: Box<DaosKeyList>,
    key_range: Range<i64>,
}

impl OpenDirState {
    fn new(dir: Arc<DaosObject>, key_list: Box<DaosKeyList>) -> Box<Self> {
        Box::new(Self {
            dir,
            key_list,
            key_range: 0..0,
        })
    }

    fn construct(
        dir: Arc<DaosObject>,
        key_list: Box<DaosKeyList>,
        key_range: Range<i64>,
    ) -> Box<Self> {
        Box::new(Self {
            dir,
            key_list,
            key_range,
        })
    }
}

#[derive(Debug)]
pub struct MetadataStore {
    root: RwLock<Option<Arc<DaosObject>>>,
    _pool: Arc<DaosPool>,
    cont: Arc<DaosContainer>,
    oid_allocator: Arc<DaosAsyncOidAllocator>,
    dir_cache: RwLock<HashMap<DaosObjectId, Arc<DaosObject>>>,
    open_dir: RwLock<HashMap<(u64, u64), Box<OpenDirState>>>,
    gen_counter: AtomicU64,
}

impl MetadataStore {
    pub fn new(
        pool: Arc<DaosPool>,
        cont: Arc<DaosContainer>,
        allocator: Arc<DaosAsyncOidAllocator>,
    ) -> Self {
        Self {
            root: RwLock::new(None),
            _pool: pool,
            cont,
            oid_allocator: allocator,
            dir_cache: RwLock::new(HashMap::new()),
            open_dir: RwLock::new(HashMap::new()),
            gen_counter: AtomicU64::new(rand::thread_rng().next_u64()),
        }
    }

    pub async fn get_node(&self, parent: DaosObjectId, entry_name: Vec<u8>) -> Result<Inode> {
        let parent_obj = self.get_dir_obj(parent, false).await?;
        let akey = vec![0u8];

        let ino_buf = parent_obj
            .fetch_async(
                &DaosTxn::txn_none(),
                DAOS_COND_DKEY_FETCH as u64,
                entry_name.clone(),
                akey,
                512,
            )
            .await?;

        if let Ok((inode, _length)) = decode_inode(ino_buf.as_slice()) {
            Ok(inode)
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Failed to decode inode, parent: {} name: {:?} buf_len: {}",
                    parent,
                    entry_name,
                    ino_buf.len()
                ),
            ))
        }
    }

    pub async fn make_node(&self, parent: DaosObjectId, name: Vec<u8>, mode: u32) -> Result<Inode> {
        let parent_obj = self.get_dir_obj(parent, false).await?;

        // FIXME: This object could be leaked if any following step failed
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
            chunk_size: DEFAULT_BLOCK_SIZE,
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
                ino_buf.bytes(),
            )
            .await?;

        Ok(inode)
    }

    fn gen_handle(&self) -> (u64, u64) {
        (
            self.gen_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            0,
        )
    }

    pub async fn open_dir(&self, dir: DaosObjectId) -> Result<(u64, u64)> {
        let dir_obj = self.get_dir_obj(dir, false).await?;
        let key_list = DaosKeyList::new();
        let dir_state = OpenDirState::new(dir_obj, key_list);
        let key: (u64, u64) = self.gen_handle();

        let mut open_dir = self.open_dir.write().await;
        open_dir.insert(key, dir_state);
        Ok(key)
    }

    pub async fn read_dir<F>(
        &self,
        handle: (u64, u64),
        offset: i64,
        mut entry_func: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>, Option<Inode>) -> Result<()>,
    {
        let mut open_dir = self.open_dir.write().await;
        let open_state = open_dir.remove(&handle);
        drop(open_dir);
        if open_state.is_none() {
            return Err(Error::new(ErrorKind::NotFound, "No handle is found"));
        }

        let OpenDirState {
            dir,
            key_list,
            key_range: range,
        } = *open_state.unwrap();

        if offset + 1 < range.start {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "offset can't move backward",
            ));
        }

        let (key_list, start_offset) = if offset >= range.end || range.start >= range.end {
            let mut key_list = dir.list_dkey_async(&DaosTxn::txn_none(), key_list).await?;
            let mut start_offset = range.end;
            while offset >= (start_offset + key_list.get_key_num() as i64) && !key_list.reach_end()
            {
                start_offset += key_list.get_key_num() as i64;
                key_list = dir.list_dkey_async(&DaosTxn::txn_none(), key_list).await?;
            }
            (key_list, start_offset)
        } else {
            (key_list, range.start)
        };

        let mut start_and_idx = (0, 0);
        for i in 0..key_list.get_key_num() {
            let (key, range) = key_list.get_key(start_and_idx)?;
            if offset <= start_offset + i as i64 {
                let inode = self.get_node(dir.oid, key.to_vec()).await?;
                entry_func(key.to_vec(), Some(inode))?;
            }
            start_and_idx = range;
        }

        let mut open_dir = self.open_dir.write().await;
        let new_range = Range {
            start: start_offset,
            end: start_offset + key_list.get_key_num() as i64,
        };
        open_dir.insert(handle, OpenDirState::construct(dir, key_list, new_range));
        Ok(())
    }

    pub async fn close_dir(&self, handle: (u64, u64)) -> Result<()> {
        let mut open_dir = self.open_dir.write().await;
        open_dir.remove(&handle);
        Ok(())
    }

    async fn get_dir_obj(&self, dir: DaosObjectId, cache_in: bool) -> Result<Arc<DaosObject>> {
        if dir == OBJECT_ID_NIL {
            return self.get_root().await;
        }

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

        let root_obj = DaosObject::open_async(self.cont.as_ref(), root_oid, false).await?;

        let duration = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d,
            Err(e) => return Err(Error::new(ErrorKind::Other, e.to_string())),
        };
        let root_inode = Inode {
            mode: make_mode(FILE_TYPE_DIR, DEF_FILE_PERM_DIR),
            oid_lo: root_oid.lo,
            oid_hi: root_oid.hi,
            atime: duration.as_secs(),
            atime_nano: duration.subsec_nanos() as u64,
            mtime: duration.as_secs(),
            mtime_nano: duration.subsec_nanos() as u64,
            ctime: duration.as_secs(),
            ctime_nano: duration.subsec_nanos() as u64,
            uid: 0,
            gid: 0,
            nlink: 1,
            chunk_size: DEFAULT_BLOCK_SIZE,
            total_size: 0,
        };
        let encoded_data = encode_inode(&root_inode).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to encode root inode: {}", e),
            )
        })?;

        let res = root_obj
            .update_async(
                &DaosTxn::txn_none(),
                DAOS_COND_DKEY_INSERT as u64,
                vec![b'.'],
                vec![0u8],
                encoded_data.bytes(),
            )
            .await;
        if res.is_err() {
            eprintln!("lose in race to create initial root object, res: {:?}", res);
        }

        let root_arc: Arc<DaosObject> = Arc::from(root_obj);
        (*root_opt).replace(root_arc.clone());
        Ok(root_arc)
    }

    fn get_container(&self) -> Arc<DaosContainer> {
        self.cont.clone()
    }
}
