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

const FILE_TYPE_MASK: u32 = 0o170000;
const FILE_PERM_MASK: u32 = 0o777;

pub fn sanitize_file_mode(raw_mode: u32) -> u32 {
    raw_mode & (FILE_TYPE_MASK | FILE_PERM_MASK)
}

pub fn get_file_type(mode: u32) -> u32 {
    mode & FILE_TYPE_MASK
}

pub fn get_file_perm(mode: u32) -> u32 {
    mode & FILE_PERM_MASK
}