# LStore-Database

### Team: Kiwi ~~Calvin~~ Bear~~s~~ Squid

#### Members: Teeranade Cheng (Win), Nikko Sanchez, Diego Rafael

## Table of Contents

- [Introduction](#introduction)
- [Implementation](#implementation)
- [Live Demo](#live_demo)
- [Credits](#credits)

## Introduction

The Lineage-based Data Store (L-Store) merges transactional and analytical processing in one engine with a new lineage-based storage, enabling seamless and contention-free updates from write-optimized to read-optimized formats while ensuring transactional integrity and historical data access.

### Data Models

LStore is a columnar database. Physical Pages make up the unit of data, containing only 8 bytes. Base Pages are used for the initial insert of records while Tail Pages handle updating of the records within Base Pages. Each record is tracked by the indirection which can then be used for traceback, getting the records historical data.

```bash
|-- Database
    |-- Table
        |-- Page Range
            |-- Base Page
                |-- Physical Pages
            |-- Tail Page
                |-- Physical Pages

#Example Case
|-- ECS165
    |--Grades
        |--PR0
            |--BP0
                |--PP0
                |--PP1
            |--BP1
            |--TP0
            |--TP1
    |--Students
        |--PR0
            |--BP0
            |--TP0
```

## Implementation

### Durability

<!-- Put MemoryDiskBuffer Image down -->
<img src="./MemoryBufferDisk.png" alt="Memory Buffer Disk Interaction"/>

- Memory
- Bufferpool
- Disk

Every operation is handled by the table which exists in memory. The Bufferpool is an small allocation of space which acts as a cache for handling data since operations done directly to a frame are costly. The disk handles durable data which is only accessible by the bufferpool when we **evict** or **import** a frame.

### Concurrency

<img src="./LocksConcurrency.png" alt="Locks and Concurrency"/>

- Locking
  - Lock Manager
  - ReadWriteLocks
- Latches
- Transactional Worker

We implemented our database using Python with **Transactional Workers** running concurrently, While transactions run sequentially since transactional workers run concurrently, the operations are considered to be concurrent. However, there are multiple issues that arises with different things accessing the same data at the same time. One such issue is called <a href="#">Race Condition</a>. To prevent this we implement **locks** (for data) and **latches** (for data structure i.e. table, page_range, lock_manager, etc)

By using this layered approach, the L-Store database efficiently handles concurrency, enabling high throughput for read operations and consistency for writes. Race conditions are averted by the strict protocol of acquiring and releasing locks through the Lock Manager, coupled with the concurrent, yet isolated, processing of transactions by individual transactional workers.

**Lock Manager**: Central to the mechanism, the Lock Manager coordinates access to database resources. It utilizes a default dictionary to associate each record ID (RID) with a corresponding ReadWriteLock.

**ReadWriteLocks**: These are objects that manage the state of each lock, including the count of read operations (num_reads) and whether a write has occurred (is_written). They offer functions to acquire and release read and write locks, ensuring that multiple reads can occur simultaneously, but write operations are exclusive.

## Live Demo

To try out our database, follow the following instructions!

Setting up:

`pip3 install` or `pip install`to install any package dependencies. The following steps uses python3 but the same could be achieved with python, depending on your python's version.

```bash
python3 testerM3_1.py
```

First part of our demo creates a `MLS3` (MileStone3) database with a `MS3` table. Inserting 1000 records using 100 transactions and 8 transactional workers which run concurrently.

```bash
python3 testerM3_2.py
```

Second part of our demo runs a tester that checks with the old database to see if the data inserted and modified within each transaction is accurate and correct.

## Credits 

As mentioned before, this was a group project that was built with sleepless nights and major caffeine addictions. Nonetheless, it was fun to build something on a lower level, manipulating, and storing data. Special thanks to @nsanchez75 and @dieg0raf for making this all happened.

Check out their works here:

- Developer: Teeranade Cheng (Win): <a href="https://www.linkedin.com/in/teeranade-cheng/" target = "_blank">LinkedIN</a> / <a href="https://github.com/winzamark123" target="_blank">Github</a>
- Developer: Diego Rafael: <a href="https://www.linkedin.com/in/diego-rafael-8668b02b1/" target = "_blank">LinkedIN</a> / <a href="https://github.com/Dieg0raf" target="_blank">Github</a>
- Developer: Nikko Sanchez: <a href="https://www.linkedin.com/in/nikko-sanchez-85888920b/" target = "_blank">LinkedIN</a> / <a href="https://github.com/nsanchez75" target="_blank">Github</a>
