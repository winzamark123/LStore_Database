# LStore-Database

### Team: Kiwi ~~Calvin~~ Bear~~s~~ Squid

#### Members: Teeranade Cheng (Win), Nikko Sanchez, Diego Rafael

## Table of Contents

- [Introduction](#introduction)
- [Diagrams](#diagrams)
- [Live Demo](#live-demo)
- [Milestones](#milestones)

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

## Implementations

### Durability
<!-- Put MemoryDiskBuffer Image down -->
<img src="./MemoryBufferDisk.png" alt="Memory Buffer Disk Interaction"/>

- Memory 
- Bufferpool
- Disk 

Every operation is handled by the table which exists in memory. The Bufferpool is an small allocation of space which acts as a cache for handling data since operations done directly to a frame are costly. The disk handles durable data which is only accessible by the bufferpool when we **evict** or **import** a frame.

### Durability

### Concurrency
