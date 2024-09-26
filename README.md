## Introduction
A Rust version of Chel-fs runs on a DAOS cluster. It consists of two bin files, chel-fs-server and chel-fs-client. chel-fs-server is the backend server. chel-fs-client is the fuse client. They together can provide a filesystem through fuse.

## How to build
The build process should be very easy except for preparation for a DAOS cluster.
### Preparations
You need to go through these steps before successfully building chel-fs-rust binaries.
* Install DAOS Cluster.

    Installing DAOS is chanllenging for new comers. But you can follow the instructions in this [document](https://github.com/chel-data/chel-fs/blob/main/docs/qemu-vms.md). It is a document for setting up a DAOS cluster on QEMU VMs on a personal computer. You can also set up a DAOS cluster on real machines. Use the official guidance from the DAOS project.

* Install library packages and tools on daos-client.

    Install Rust. You can install Rust by following the instructions on the [Rust website](https://www.rust-lang.org/tools/install).

    Install libfuse. You need to install both library and development package. For Rocky Linux you can use "dnf install fuse3-libs fuse3-devel". For Ubuntu you can use "apt-get install libfuse3-3 libfuse3-dev".

    Install protobuf and protobuf-compiler. Use the package manager of your OS to install it.

### Checkout Code
Check out these repos on daos-client.

    git clone git@github.com:chel-data/daos-rust-bind.git
    git clone git@github.com:chel-data/chel-fs-rust.git

chel-fs-rust depends on daos-rust-bind. They should be put in the same folder. When you build chel-fs-rust cargo will know where to find daos-rust-bind.

### Build Binaries
You should build it on your daos-client node. Go to the chel-fs-rust directory and run "cargo build".

### Run chel-fs
Running chel-fs is easy. First make sure the DAOS cluster is properly running. You should have a pool named "pool1" and a POSIX container named "cont1". You can use this little script to set up the pool and container.

    #!/usr/bin/bash
    dmg storage format
    sleep 10
    dmg pool create pool1 --size 12G
    daos pool query pool1
    daos cont create -t posix pool1 cont1

Then run "cargo run --bin chel-fs-server" first. Create a empty folder /mnt/fs2 as the mountpoint of the fuse filesystem. Open another console and run "cargo run --bin chel-fs-client". Go to /mnt/fs2. Now you can play with this little filesystem. When you want to restart or stop the filesystem, use "fusermount3 -u /mnt/fs2" to stop the fuse mountpoint.
