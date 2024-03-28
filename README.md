# mktosync
Public repository for Marketo sync tool. This helper utility is something I wrote to learn Ruby language. The key initial proglem to solve was the way how Marketo provides its data only in batches. At the time, I did not have a suitable iPaaS or data extraction service available to handle that, so I took this as an opportunity to build a proof-of-concept, and also learn Ruby basics.

Since then (about 2020) I'm sure things have changed, and likely very good solutions can be found for this purpose.

The app is by no means production grade for any serious use, but might provide someone interested with a starting point or template for something more defined. For instance, only a handful of lead fields are extracted.

Some key features and functionalities:

- Accesses Marketo data using Marketo REST API using module 'httparty'.
- Multithreaded; data request API requests to Marketo, polling for the results of the reqeusts, and downloading data and writing to target database are in their own threads.
- Logging using 'logger' module.
- Support Micorosft SQL for both the target data storage, as well as tool status. Using modules 'sequel' and 'connection_pool' and 'tiny_tds'.
- Configuration in separate JSON file, related JSON handling.
