# logstats

This library provides a go-lang implementation for logging of statistical information to log files.

Every application maintains a set of metrics (or stats) to ensure better debuggability and better reporting of application health to the user. Some applications log the stat values in the log files, whereas other applications use some database to store the stat values. The applications which log the stats in the log files, just use some language-native or os-native logging frameworks, which are primarily designed to store completely unstructured data. But the stats are at least partially - if not fully - structured. So, native logging frameworks fall short in exploiting the structured-ness of the stats while storing the information on the disk.

This library provides features like stat-deduplication to save disk space. Saving of disk space comes with following advantages.

1. Due to stat-deduplication, more amount of debug information becomes available with same amount of space requirement - as compared to non-deduplicated stats.
2. Collection of information (log files) becomes faster - due to saving of network bandwidth. This saves very important minutes during mission critical support cases.

# How to use

There are two functionalities supported by this logstats library.

1. Stats logging with log rotation WITHOUT deduplication
2. Stats logging with log rotation WITH deduplication

To create a stats logger WITHOUT deduplication, use:

```
func NewLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*logStats, error)
```

To create a stats logger WITH deduplication, use:

```
func NewDedupeLogStats(fileName string, sizeLimit int, numFiles int, tsFormat string) (*dedupeLogStats, error)
```

To write the stats to the log file, use:

```
(*logStats) Write(statType string, statMap map[string]interface{}) error
(*dedupeLogStats) Write(statType string, statMap map[string]interface{}) error
```

To enable/disable "flush to the disk" after every subsequent call to Write, use:

```
(*logStats) SetDurable(durable bool)
(*dedupeLogStats) SetDurable(durable bool)
```

## Supported Types for deduplication

The following types are supported for deduplication in the supplied map argument -

-   int64/uint64
-   bool
-   string
-   map[string]interface{} - nested stats
-   Timestamp (custom type) - refer below for more details

Timestamp is a custom type for Timestamp based logging where we want to log something when timestamp changes.
It accepts a custom marshal func which is called during logging. The default Marshal behaviour is to log the duration since the Timestamp.

# Caveats

## File Size Limit

Unlike language-native logger framework implementations, in this framework sizeLimit is not a hard limit for the log file sizes. On contrary, it is just a soft limit. This becomes useful in

1. Ensuring availability of the in case of deduplication of the stats as well as
2. Handling use case of very small size limit versus very large log messages.

For example, if the size limit is 100 and the first log message is 128 bytes, the entire log message will be written to the file - as this logging framework does not break up the log messages across multiple files. Recommendation is to use multiple small sized stat map (instead of using as single very large stats map), so that the excess bytes written to the file, beyond size limit will be limited. statType parameter - in `Write` interface- can be used to divide the stats among multiple sub-stats.

## Supported data types for deduplication.

Currently, deduplication is supported only for the values of type `int64`, `string`, `uint64`, `bool` and `nested map`. In case of the nested maps, deduplucation for values within nested maps is supported.

## Single Process Access

It is recommended to use this logging framework with one log file being used by only 1 process. Same log file being used by multiple processes can cause unexpected results.

# How deduplication works?

The stats deduplication will happen only within a single file. Once the file gets rotated, the log messages will not get deduplicating across multiple files.

For example, for the initial stat values `{"k1": 10, "k2": 20, "k3": 30}`, if only the value of `"k1"` changes to `100`, then the next log message in the same log file will be `{"k1": 100}`. Note that `"k2": 20, "k3": 30` is not written to the file due to deduplication. But if the next log message were to be written to the different log file (due to log rotation), entire set of stats - `{"k1": 100, "k2": 20, "k3": 30}` - will be written to the new (rotated) log file.

Also note that, deduplication is not preserved across logger object re-initializations. This means if the logger object is re-initialized from an earlier state (situations like process restart), deduplication starts afresh, i.e. first log message will be written without any deduplication.

# Performance Guidelines

## Avoid very large stats maps

If the stats maps to be logged are very large, it may take equivalent amount of time to deduplicate it against the previous stats map. So, recommendation is to log smaller chunks of stats within a single call to `Write` interface. If needed, stats can be sub-divided in multiple types of stats by providing different statType. For example, memory stats and CPU stats of a process can be written separately in two different calls to `Write` interface.

## Avoid very high log frequency

This library is not optimized for very high frequency stat logging. So, recommendation is to log stats every 15 seconds or even less frequently than that.

# Example
