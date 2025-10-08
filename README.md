# Payments Engine

A toy implementation in rust of a simple payments engine that processes transactions from a CSV file and updates the account balances accordingly.
It supports account deposits, withdrawals and disputes on deposit transactions.

## Building and running the application

To run the application you need to have `cargo` installed.
The application accepts a single parameter which is the CSV file with the input transactions. Run the application like this:
```
$ cargo run -- test_input.csv
```

## Design

The following diagram showcases the design of the application.
![Design Diagram](design.svg)

The input file is parsed by the `csv_reader` module. There is a `Transaction` structure that matches the fields in the CSV file. A custom deserializer is used in order check if the amount value in the CSV is negative and also to round it to 4 decimal places.
The application will skip any row that has a negative amount or an invalid format. Also amounts that are not rounded to 4 decimal places will be automatically rounded (e.g. `1.9999999` will be rounded to `1.9999` and `1.49999` will be rounded to `1.4999`).

The application accepts inputs that have the header specified in the file `type, client, tx, amount` but will accepts files that don't have the header as long as the order of the fields is preserved in each row. Each row that fails to de-serialize will be ignored by the application.

The CSV reader uses an iterator to iterate over every single row. Once an entry in the file is parsed, it is sent to a worker task for processing.
There is a stable set of workers that are spawned when the application starts and they will continue running until the input is finished. Each worker serves a set of clients. To determine which worker should serve a client, a simple hash function is used.

The `transaction_processor` module contains the logic to process transactions. It reads transaction messages from a queue. It also holds one or more accounts and processes each message accordingly.
If an error occurs with a transaction, it will be logged to stderr and the processor will continue with the next transaction.

The business logic used to update the balances of the account is contained in the `account` module, more specifically the `Account` struct. This struct contains methods for depositing, withdrawing, disputing, resolving disputes and issuing chargebacks.
There are a number of errors that can happen when processing transactions which are specified in the `AccountError`.

The assumptions are that:
* no transactions can be processed if the account is locked
* a withdrawal cannot happen if there's not sufficient available balance
* disputes can only be issued for deposits. Disputing withdrawals is not supported by the application. This seems in line to what payment processors usually do. There might be situations where disputes on withdrawals can happen but it's usually implementation specific what happens in those cases. Usually it would produce a hold but there are weird cases where the customer would not be allowed to use available balance even if it is positive do to that hold. This application does not support that.
* disputes must happen after a transaction has been processed. Disputes on non-existing transactions are not supported (or for that matter out of order disputes).
* a dispute on a transaction can only happen once. If the dispute is resolved, the transaction cannot be disputed again.
* any chargeback locks the account.
* it's possible for the account to have negative balance. This may happen as a result of a chargeback. This is in line with what other payment processors implement.

Because there can be billions of transactions that cn be processed for an account, in order to limit the amount of memory used, each account stores the previous transaction log in a cached transaction store that is backed on disk.
This cache will store deposit and withdraw transactions and will keep only the most recently used transactions in memory. Each cache is initialized with a fixed capacity. When this size is exceeded the least recently used items are evicted from memory into a backing store database.

There are several implementations for the backing store database in the `transaction_cache` module. This is because the implementation was started using `sled` as a backing store which turned out to consume more memory than expected. The next storage backend implemented was `rocksdb` which worked well to limit memory usage but was really slow to compile. The default implementation now uses a simple KV store implemented using SQLite. There is still support for the `rocksdb` implementation using an optional feature.
Another implementation that was considered was to encode each transaction with bincode and serialize it to disk in a separate file (the filename would be the transaction id). Ultimatelly this may be problematic since the number of files may be exceeded on some filesystems. It would be better to bundle up multiple transactions in a single file but that would mean either implementing an index or searching linearly through the file (on a slow media). Instead of re-inventing the wheel I chose to evaluate well established KV storage options.

The `test_cache_memory_usage` integration test was used to validate the memory consumption. It's also the reason that the `transaction_cache` module is public.

## Planned improvements

Although the payment workers are async tasks, the operation that does the most IO which is the eviction of the transactions to disk does not currently use an async interface. It's worth implementing an async interface for the cache in the future.

The current scaling strategy of the application is to distribute distinct clients into distinct workers. This would provide a more uniform QOS for clients so that it reduces the posibility that one clients transactions are staving another ones. Still it can happen that on a worker that is serving 2 or clients, one client who put in a transaction later is starved by a client that has issued a large number of transactions before that.
A priority scheme with a more fair QOS can be implemented potentially per worker.

The cache can be improved in order to support bulk eviction. Now the cache evicts one entry at a time to disk when it reaches its capacity limit. A bulk eviction would make more sense, especially in the case where transaction IDs are ordered.

A more comprehensive test suite needs to be implemented also.

## Testing
There are 35 unit tests implemented that cover mainly account functionality, csv parsing, smoke tests for the cache and newtypes.
There are 2 integration tests that check large inputs that were generated using the help of ChatGPT.
Under the `testing/inputs` directory, there are 14 input files that emulate different scenarios. These were also generated with the help of ChatGPT.

The `test_cache_memory_usage` integration test is used to debug memory usage of the caches. This is needed because it uses a tracking global allocator to account for the allocated size.

## Crates used

* rust_decimal - suitable for financial calculations; ~57M downloads, activelly maintained
* bincode - binary encoding library; ~152M downloads, activelly maintained
* lru - cache implementation; ~133m downloads, activelly maintained
* rocksdb - database; ~31M downloads, activelly maintained
* rusqlite - database; ~38M downloads, activelly maintained
* thiserror - convenience for error definition; ~568M downloads, activelly maintained
* tempfile - temporary file manager crate; ~358M downloads, activelly maintained
