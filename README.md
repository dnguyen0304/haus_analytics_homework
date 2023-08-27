# Haus Analytics Homework

## Tl;DR

- Use conflict detection multi-version concurrency control (MVCC) for
  transactions, which trades space via its immutable, append-only internal data
  structure for time.

- Use asyncio as the web server for non-blocking concurrency.

## Getting Started

### Installing

There are only first-party dependencies, so you don't actually need to install
anything if you are already running Python 3. This code was tested on
Python 3.10.

```
python3 -m venv env
source env/bin/activate
python3 -m pip install --upgrade pip setuptools
```

### Hello, World!

```
# server
cd haus_analytics_homework/
python src/server.py
```

```
# client #1
nc localhost 5000
GET intro
```

```
# client #2
nc localhost 5000
START
PUT name duy
```

### Testing

```
python3 -m pip install --upgrade pytest
pytest --capture=no
```

### Type Checking

```
python3 -m pip install --upgrade mypy
mypy haus_analytics_homework
```

## Details

### Guiding Principles

- Assume there are **no novel problems** being introduced here. I tried to
  associate each part of the homework with a known, solved problem in computer
  science.

- Assume someone **smarter than me** has already solved this problem. I tried to
  think about how variations of the homework may have appeared before in the
  industry and used those as "reference architectures".

- Optimize for an **interview setting** rather than production. In production
  and with so little context, it would probably be unwise to implement something
  this complex for the first iteration. Better understanding the problem space,
  reviewing user data, and starting with a "good enough" solution such as
  slapping locks everywhere would have been more agile.

- It's the **weekend**. It's very thoughtful of the Haus team being oncall to
  field my clarifying questions, but I think respecting my teammates' time sets
  an important precedent. In addition, I thought the problem's ambiguity was
  manageable and wouldn't leave me blocked for hours.

### Design Decisions

#### Large

- The problem statement mentions "transactions". At first, I thought this
  problem was a distributed transactions problem. However, I then felt it was
  better suited for a **centralized transactions** problem. NoSQL databases such
  as ElasticSearch and Cassandra forgo this feature in most of their workloads.
  SQL databases have been solving this problem for decades though. I read the
  internals for PostgreSQL multi-version concurrency control and implemented a
  simplified version here.

- The problem statement mentions "high-performance cache". Python has a couple
  concurrency models, but **async I/O** is a strong candidate because updating
  the internal data structures is lockless, requires no synchronization, and is
  fast. Therefore, cooperative multitasking alleviates our bottleneck.
  Alternatives include threading (acceptable, but 10X to 100X more
  resource-intensive), multiprocessing (serializing data between processes alone
  would be enough of a hindrance), and mutex/semaphore locks (more on this
  later). Redis is an excellent reference architecture here. Statements from the
  original maintainer indicate a non-blocking event loop paired with efficient
  data structures was enough to achieve good performance.

#### Medium

- Use **READ_COMMITTED** as the isolation level. This is a reasonable default
  used by databases such as PostgreSQL, Azure, and Oracle. How extensible is the
  code if we needed to configure this? We would need something similar to a
  strategy pattern.

- Assume locking at the **row-level**. Without table-level locking, we cannot
  achieve serializable isolation level or stronger, but that trade-off affords
  us significantly more throughput.

- Assume transactions do not live longer than **low, single-digit seconds**, but
  assume it is possible to have 100s of transactions at that magnitude.
  Therefore, we are heavily I/O-bound and if mutex/semaphore locking is used, we
  would also incur CPU-bound by work such as spinlocking.

#### Small

- The **snapshot isolation** is at the query level. In other words, queries
  within the same transaction are visible to one another. Some implementations
  are stricter about isolation. I elected for this approach because I believe it
  is a more predictable user experience. It shows the user their understanding
  of the world.

- The in-memory database is not **durable**. A common database strategy is a
  write-ahead log that persists changes to disk before merging with the actual
  data. This im-memory implementation is not resilient against issues such as
  hardware failure.

- **Integration tests** are particularly valuable in this problem space.
  Transactions are intrinsically a unit of work that involves branching paths
  and conflicts.

### Alternatives Considered and Non-Requirements

- The most common alternative approach for implementing transactions is
  **pessimistic locking**. Another way to view the two approaches is conflict
  _avoidance_ and conflict _detection_. After further research, pessimistic
  locking is much slower. I believe our use case can afford the compromise
  because of (1) the simple data type, (2) no multi-record operations, and (3)
  no sub-row operations.

- MVCC requires a **garbage collection** process called a "vacuum". For example,
  you "stop the world", then delete all non-active and non-committed changes. We
  would need to migrate from arrays to linked lists for O(1) deletes from the
  head.

- What happens if the cache grows very large and you _don't_ want to shard
  across multiple machines? You can implement a **range-based** hashing
  approach. Instead of 1 thread managing keys from [a-zA-Z0-9], you could have
  _n_ threads managing (26 + 26 + 10) / _n_ keys.

- Command-line help, B-tree indexing, additional data types, inline code
  documentation, predicate matching (`WHERE`), better error handling, and
  more...
