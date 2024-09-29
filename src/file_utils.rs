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

use fuser::FileType;

pub const FILE_TYPE_MASK: u32 = 0o170000;
pub const FILE_PERM_MASK: u32 = 0o777;
#[allow(unused)]
pub const FILE_TYPE_REG: u32 = 0o100000;
#[allow(unused)]
pub const FILE_TYPE_DIR: u32 = 0o040000;
#[allow(unused)]
pub const FILE_TYPE_SYM: u32 = 0o120000;
#[allow(unused)]
pub const DEF_FILE_PERM_REG: u32 = 0o644;
#[allow(unused)]
pub const DEF_FILE_PERM_DIR: u32 = 0o755;

pub const DEFAULT_BLOCK_SIZE: u32 = 1 << 16;

#[allow(unused)]
pub fn get_file_type(mode: u32) -> FileType {
    match mode & FILE_TYPE_MASK {
        FILE_TYPE_REG => FileType::RegularFile,
        FILE_TYPE_DIR => FileType::Directory,
        FILE_TYPE_SYM => FileType::Symlink,
        _ => FileType::RegularFile,
    }
}

#[allow(unused)]
pub fn get_file_perm(mode: u32) -> u32 {
    mode & FILE_PERM_MASK
}

#[allow(unused)]
pub fn apply_umask(mode: u32, umask: u32) -> u32 {
    let perm = mode & FILE_PERM_MASK;
    let file_type = mode & FILE_TYPE_MASK;
    perm & !umask | file_type
}

#[allow(unused)]
pub fn make_mode(file_type: u32, perm: u32) -> u32 {
    (file_type & FILE_TYPE_MASK) | (perm & FILE_PERM_MASK)
}
