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
Archive staging is a separate class, `GCSArchiveDagBundle`, with
`archive_object` — the same split as
[`S3ArchiveDagBundle`](https://github.com/apache/airflow/pull/73015)
([`ad0592a`](https://github.com/apache/airflow/pull/73015/commits/ad0592a555cdc1110e006b5c8b989a5e02e634a6)).
`GCSDagBundle` stays the prefix path and has no `archive_object` argument.
The Google provider in this image (22.3.0) does not ship the archive class
yet ([apache/airflow#75566](https://github.com/apache/airflow/pull/75566)),
so `bundle_pkg/` vendors it. The live config uses that package. Importing it
returns `airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle` once
that class is installed; point the bundle classpath at the provider then.

Everything runs on your machine: `astro dev` (Airflow 3.3, same runtime as
the S3 reproduction) plus
[fake-gcs-server](https://github.com/fsouza/fake-gcs-server). **No GCP
project, no cloud requests, nothing is uploaded anywhere.**

```
per-object sync (GCSDagBundle)           single archive (GCSArchiveDagBundle)

worker start                             worker start
  │                                        │
  ├─ GET  bucket                           ├─ GET  archive metadata (ETag, generation)
  ├─ LIST prefix                           ├─ GET  archive.tar.gz     ─┐
  ├─ GET  file_001                   ─┐    │     ifGenerationMatch     │
  ├─ GET  file_002                    │    ├─ verify + unpack          │ ~fast
  ├─ ...                              │slow ├─ swap into place        ─┘
  ├─ GET  file_400                   ─┘    task code runs
  task code runs
```

On a warm disk (the scheduler, after the first refresh) the stock loop is
different but still per-object: `GCSHook` calls `blob.reload()` for every
file that already exists, so an unchanged bundle still costs about N metadata
GETs. The archive path re-reads one object's ETag. It skips the download when
that ETag matches the marker beside the bundle directory and the directory
still has files. An empty directory is restaged even when the ETag matches.

## Why the same shape as S3

`GCSHook.sync_to_local_dir()` is the same serial loop as
`S3Hook.sync_to_local_dir()`: list the prefix, then download each object on
the calling thread. There is no concurrency knob on that path. The archive
class does not take `prefix`. The archive is the only source of the DAGs. A
failed stage raises and leaves the previously staged tree in place. Between
the two renames the bundle directory is briefly missing. Publishing the
archive is the deployer's job
(`tar -C ./dags -czf dags.tar.gz .` and `gcloud storage cp`).

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
  round 1:   0.87s   (download=400, list=2, bucket=1)
  round 2:   0.82s   (download=400, list=2, bucket=1)
  round 3:   0.80s   (download=400, list=2, bucket=1)

[2/2] GCSArchiveDagBundle — single tar.gz object (gcs_archive_bundle.GCSArchiveDagBundle)
  round 1:   0.06s   (download=1, metadata=1, bucket=1)
  round 2:   0.06s   (download=1, metadata=1, bucket=1)
  round 3:   0.06s   (download=1, metadata=1, bucket=1)

==========================================================================
scenario                                     median  GCS reqs   files
--------------------------------------------------------------------------
per-object sync (stock GCSDagBundle)          0.82s       403     400
single archive (GCSArchiveDagBundle)          0.06s         3     400
==========================================================================

speedup: 13.5x   (every KubernetesExecutor task pod pays the cold-staging cost)
```

The request mix did not move between rounds. Stock cold staging is 1 bucket
GET, 2 lists (the prefix check, then the refresh), and 400 media downloads.
This archive class does not probe `exists()` before `reload()`. A cold stage
is 1 bucket GET, 1 metadata read (ETag and generation), and 1 media download
pinned with `ifGenerationMatch` — 3 calls, matching the rounds above.
`just bench` wipes only the bundle directory. The ETag marker sits beside it
(`.{name}/etag`), and an empty directory is restaged, so each round still
downloads the archive instead of skipping on a leftover marker. The archive
stays at that fixed call count against real GCS. The stock cost scales with
file count times round-trip time.

Loopback fake-gcs makes each stock call about 2 ms, so 13.5× wall clock is
the floor. The S3 reproduction of the same 400 files was ~16 s against
MinIO and 20+ s against real S3, because that path also HEAD-checks every
object. Neither number is a cloud bill: nothing left the machine.

## Poke around

```bash
just logs    # how the DAG processor staged the two live bundles
just trace   # proxy access log (errors only; counts are in the benchmark)
just bench 5 # more rounds
curl -s localhost:18888/__metrics
```

The Airflow UI is at http://localhost:18080. Both bundles are configured in
[.env](.env). They differ by class and by source argument:

| | per-object | archive |
|---|---|---|
| name | `gcs-per-object` | `gcs-archive` |
| classpath | `airflow.providers.google.cloud.bundles.gcs.GCSDagBundle` | `gcs_archive_bundle.GCSArchiveDagBundle` (provider classpath `airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle` when that class is installed) |
| source | `prefix`: `dags/` | `archive_object`: `archive/dag-bundle.tar.gz` |

`GCSDagBundle` does not accept `archive_object`. `GCSArchiveDagBundle` does
not accept `prefix`. The `bundle_demo` DAG registers under one of them (DAG
ids are global).

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
  calls `blob.reload()` first. That class is unchanged here.
- `GCSArchiveDagBundle` is the archive class proposed for
  `airflow.providers.google.cloud.bundles.gcs` in
  [apache/airflow#75566](https://github.com/apache/airflow/pull/75566),
  following the `S3ArchiveDagBundle` split in
  [apache/airflow#73015](https://github.com/apache/airflow/pull/73015).
  Until the provider ships it, the same shape lives in `bundle_pkg/`.

## Layout

```
bundle_pkg/            vendored GCSArchiveDagBundle (used until the provider class is installed)
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
`GCSDagBundle` imports. The per-object class is that upstream class. The
archive class is the vendored `GCSArchiveDagBundle` until the provider ships
`airflow.providers.google.cloud.bundles.gcs.GCSArchiveDagBundle`. The full
provider also depends on Ray, pandas, and every Google product client; those
are not imported by the bundle path and are not installed here.

## Notes

- Members are extracted relative to the bundle root, so pack the *contents* of
  the DAGs directory (`tar -C ./dags -czf dags.tar.gz .`) rather than the
  directory itself.
- Unpacking uses `tarfile` `filter="data"` (Python 3.11.4 or later).
  Constructing `GCSArchiveDagBundle` on an older interpreter raises
  `RuntimeError`.
- The ETag marker is `.{bundle name}/etag`, next to the bundle directory.
  Staging directories live there too, and leftover `archive-staging-*`
  directories for this bundle are removed on refresh. An empty bundle
  directory is restaged even when the marker still matches.
- The download passes `if_generation_match` for the generation read with the
  ETag. A mismatch raises and leaves the previous tree in place.
- The swap is two renames. A half-unpacked tree is not left behind. Between
  the renames the bundle directory does not exist.
- Delete the archive object and rerun `just bench` to watch the archive
  bundle fail. It does not fall back to the per-object prefix.
