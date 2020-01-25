# FoundationDB Block Device
Replicated Block Device backed by FoundationDB

## What is this
This is an implementation of a block device in userspace which uses FoundationDB as a backend.
It provides a replicated block device for non-replicated workloads so they 
can benefit from transparent block-level replication and enhanced fault tolerance.

Inspired by [spullara/nbd](https://github.com/spullara/nbd)

## Is it fast?
I did a small benchmark using a FoundationDB cluster of 2 nodes (linux running on macbooks with SSDs, 
not tuned for FDB at all).
FIO benchmark on 1GB file resulted in 10K random read/write IOPS in 4KB blocks and the latency was below 10ms (direct io was used).
While doing sequential reads it was able to saturate 1Gbit network link.

Postrgres running in virtualbox showed 900 TPS on TPC-B pgbench workload with a database of size 1g.

## Current status
It's a prototype. There are several important featues which are not implemented yet 
(such as fencing and volume size estimation) but it works and it's relatively fast!

## How to use
Commands are documented in the CLI:
```
$ ./fdbbd --help
NAME:
   fdbbd - block device using FoundationDB as a backend. 
   Our motto: still more performant and reliable than EBS

USAGE:
   fdbbd [global options] command [command options] [arguments...]

VERSION:
   0.1.0

COMMANDS:
     create   Create a new volume
     list     List all volumes
     attach   Attach the volume
     delete   Delete the volume
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h     show help
   --version, -v  print the version
```

## Getting started

1. Set up a FoundationDB cluster.
2. Build the driver: 
```sh
sh build.sh
```
3. Create a new volume:
```sh
$ ./fdbbd create --size 1GB myvolume
```
4. If `nbd` kernel module is not loaded, load it:
```sh
$ sudo modprobe nbd
```
5. Attach the volume to the system:
```sh
sudo ./fdbbd attach --bpt 4 myvolume /dev/nbd0
```
6. Create a directory to mount the volume:
```sh
mkdir nbdmount
```
7. Create a file system on your block device. XFS is a good option:
```sh
sudo mkfs.xfs /dev/nbd0
```
8. Mount the attached volume:
```sh
sudo mount /dev/nbd0 nbdmount/
```
9. Done! You have a replicated volume!

## What's inside
This project uses Network Block Device kernel module underneath. A unix pipe is used to talk to a kernel,
and then driver translates NBD protocol into FoundationDB calls.

## Roadmap
There are a few features planned in future releases, ordered by importance:

1. Fencing every FDB transaction to protect block devices from being shared
2. IOPS isolation
3. CSI implementation
4. Snapshots
5. Volume size estimation (using roaring bitmaps or similar)
6. Client-side encryption
7. Control panel
