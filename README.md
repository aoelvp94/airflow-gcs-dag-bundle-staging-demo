# Airflow GCS DAG bundle staging: per-object sync vs single archive

A self-contained, fully local reproduction of the same gap as
[apache/airflow#73015](https://github.com/apache/airflow/pull/73015), on GCS
instead of S3. **`GCSDagBundle` stages the bundle one object at a time,
serially**, so a DAG project made of hundreds of small files pays one GCS
round-trip per file on every fresh worker. Under KubernetesExecutor every
task instance is a fresh worker, which puts the full staging cost on the
critical path of every task start.

The S3 reproduction is
[aoelvp94/airflow-s3-dag-bundle-staging-demo](https://github.com/aoelvp94/airflow-s3-dag-bundle-staging-demo).
`GCSDagBundle` has no archive option, so `bundle_pkg/` subclasses it and
changes only the staging strategy. `archive_object` is mutually exclusive
with `prefix`.

Everything runs on your machine: `astro dev` (Airflow 3.3, same runtime as
the S3 reproduction) plus
[fake-gcs-server](https://github.com/fsouza/fake-gcs-server). **No GCP
project, no cloud requests, nothing is uploaded anywhere.**

```
per-object sync (stock)                  single archive (this PoC)

worker start                             worker start
  │                                        │
  ├─ GET  bucket                           ├─ GET  archive metadata (md5)
  ├─ LIST prefix                           ├─ GET  archive.tar.gz     ─┐
  ├─ GET  file_001                   ─┐    ├─ verify + unpack          │ ~fast
  ├─ GET  file_002                    │    ├─ atomic rename swap      ─┘
  ├─ ...                              │slow task code runs
  ├─ GET  file_400                   ─┘
  task code runs
```

On a warm disk (the scheduler, after the first refresh) the stock loop is
different but still per-object: `GCSHook` calls `blob.reload()` for every
file that already exists, so an unchanged bundle still costs about N metadata
GETs. The archive path re-reads one object's md5 and skips the download when
it matches.

## Why the same shape as S3

`GCSHook.sync_to_local_dir()` is the same serial loop as
`S3Hook.sync_to_local_dir()`: list the prefix, then download each object on
the calling thread. There is no concurrency knob and no archive mode.
`archive_object` and `prefix` are mutually exclusive. The archive is the only
source of the DAGs. A failed stage raises and leaves the previously staged
tree in place. Publishing the archive is the deployer's job
(`tar -C dags . | gcloud storage cp - gs://bucket/archive/dag-bundle.tar.gz`).

The cold path is what a KubernetesExecutor pod pays. Loopback fake-gcs hides
most of the network round-trip; against real GCS each of those serial
requests adds tens of milliseconds, which is why the S3 measurements on real
S3 were slower than the MinIO numbers. Request *count* is the part that does
not depend on the emulator.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`brew install astro`)
- [just](https://github.com/casey/just) (`brew install just`)
- Python 3.10+ on the host (stdlib only, used to generate and seed the bundle)

Host ports: Airflow UI `18080`, Postgres `15432`, fake-gcs `4443`, request
counter `18888`. Chosen so this project can run next to another local Airflow.

## Reproduce (one command)

```bash
just e2e
```

That runs, in order:

| step | recipe | what it does |
|---|---|---|
| 1 | `just generate` | builds `bundle_build/`: 400 small `.py` files (~3 MiB) **and** a `.tar.gz` of the same files |
| 2 | `just up` | `astro dev start`: Airflow 3 + fake-gcs + a counting proxy |
| 3 | `just seed` | creates bucket `dag-bundles`, uploads the files per-object **and** the archive — to local fake-gcs only |
| 4 | `just bench` | stops the DAG processor so it cannot add requests, then cold-stages both bundle classes 3× |

Each benchmark round deletes the bundle's local directory first. That is the
fresh-worker simulation. Measured on this machine (Airflow 3.3.1, provider
`apache-airflow-providers-google` 22.3.0, fake-gcs 1.52.3, everything on
localhost):

```
Cold-staging benchmark, 3 rounds each (local dir wiped before every round)

[1/2] stock GCSDagBundle — serial per-object sync
  round 1:   1.10s   (download=400, list=2, bucket=1)
  round 2:   0.93s   (download=400, list=2, bucket=1)
  round 3:   1.52s   (download=400, list=2, bucket=1)

[2/2] GCSArchiveDagBundle — single tar.gz object
  round 1:   0.17s   (download=1, metadata=2, bucket=1)
  round 2:   0.20s   (download=1, metadata=2, bucket=1)
  round 3:   0.40s   (download=1, metadata=2, bucket=1)

==========================================================================
scenario                                     median  GCS reqs   files
--------------------------------------------------------------------------
per-object sync (stock GCSDagBundle)          1.10s       403     400
single archive (GCSArchiveDagBundle)          0.20s         4     400
==========================================================================

speedup: 5.4x   (every KubernetesExecutor task pod pays the cold-staging cost)
```

The request mix did not move between rounds. Stock cold staging is 1 bucket
GET, 2 lists (the prefix check, then the refresh), and 400 media downloads.
The archive is 1 bucket GET, 2 metadata reads (object exists, then md5), and
1 media download.

Loopback fake-gcs makes each call about 3 ms, so 5.4× wall clock is the
floor. The S3 reproduction of the same 400 files was ~16 s against MinIO and
20+ s against real S3, because that path also HEAD-checks every object.
Against real GCS the stock cost scales with file count times round-trip
time. The archive stays at 4 calls either way. Neither number is a cloud
bill: nothing left the machine.

## Poke around

```bash
just logs    # how the DAG processor staged the two live bundles
just trace   # proxy access log (errors only; counts are in the benchmark)
just bench 5 # more rounds
curl -s localhost:18888/__metrics
```

The Airflow UI is at http://localhost:18080. Both bundles (`gcs-per-object`
and `gcs-archive`) are configured in [.env](.env). The `bundle_demo` DAG
registers under one of them (DAG ids are global).

When you're done: `just down` (stop) or `just clean` (delete everything).

`astro dev start` can report *"The api-server health check timed out"* on a
slow first boot. The containers keep starting; `docker ps` to confirm. The
DAG processor also logs that the archive does not exist until `just seed`
has run. `just e2e` starts Airflow before seeding, so that window is
expected.

## Where the behaviour lives upstream

- [`GCSDagBundle.refresh()`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/bundles/gcs.py)
  → [`GCSHook.sync_to_local_dir()`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/hooks/gcs.py):
  a serial `for blob in bucket.list_blobs(...)` loop, one blocking download
  per object. If the local file already exists, `_sync_to_local_dir_if_changed`
  calls `blob.reload()` first.
- The S3 counterpart is
  [`S3DagBundle`](https://github.com/apache/airflow/blob/main/providers/amazon/src/airflow/providers/amazon/aws/bundles/s3.py),
  with the archive option proposed in
  [apache/airflow#73015](https://github.com/apache/airflow/pull/73015).

## Layout

```
bundle_pkg/            the GCSArchiveDagBundle proof of concept (pip-installed into the image)
scripts/generate_bundle.py   synthetic bundle generator (host, stdlib only)
scripts/seed_gcs.py    upload both layouts to local fake-gcs
scripts/gcs_proxy.py   counts GCS requests, then forwards to fake-gcs
include/benchmark.py   in-container benchmark (timings + request counts)
docker-compose.override.yml  adds fake-gcs and the counting proxy
.env                   anonymous GCP connection, emulator host, live bundle config
justfile               all of the above, wired end to end
```

The image installs `apache-airflow-providers-google==22.3.0` (the version
pinned by Airflow 3.3.1 constraints) with `--no-deps`, then only the libraries
`GCSDagBundle` imports. The class under test is the upstream one. The full
provider also depends on Ray, pandas, and every Google product client; those
are not imported by the bundle path and are not installed here.

## Notes

- Members are extracted relative to the bundle root, so pack the *contents* of
  the DAGs directory (`tar -C dags .`) rather than the directory itself.
- The PoC extracts with `tarfile`'s `filter="data"` and swaps directories by
  rename, so a partially staged tree is never what the worker reads.
- `prefix` and `archive_object` are mutually exclusive. Delete the archive
  object and rerun `just bench` to watch the archive bundle fail instead of
  staging from the per-object prefix.
