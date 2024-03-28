# mktosync
Public repository for Marketo sync tool. This helper utility is something I wrote to learn Ruby language. The initial problem it was intended to solve was related to some challenges in easily syncing all Marketo data into a data warehouse, and the goal was to:

A) Learn Ruby with a real-life use case.
B) Build a proof-of-concept to see how feasible if would be to dump Marketo data with a custom uitily.

Since then (about 2020) I'm sure things have changed, and likely very good solutions can be found for this purpose.

The app is by no means production grade for any serious use, but might provide someone interested with a starting point or template for something more defined.

Some key features and functionalities:

- Accesses Marketo data using Marketo REST API using module 'httparty'.
- Multithreaded; data reads from Marketo, polling status of previous API reqeusts, and writing to target database are in their own threads.
- Logging using 'logger' module.
- Support Micorosft SQL for both the target data storage, as well as tool status. Using modules 'sequel' and 'connection_pool' and 'tiny_tds'.


