# Knime Extension Nodes for OrientDB
The OrientDB nodes make it possible to connect to dedicated database and execute create, read, update and delete operations (CRUD).
OrientDB is a multi-model open source NoSQL database management system that combines the power of graphs with document, key/value, reactive, object-oriented, and geospatial models into a single scalable, high-performance operational database (https://orientdb.com/).


The are five OrientDB nodes in OrientDB extension:
* Connection node: a node that allows you to create a connection to a remote or local OrientDB server. Here the user can specify the location and port of the database, its name, provide login and password, or use KNIME credentials. Once the connection is successfully created it can be propagated to other OrientDB nodes.
* Query node: to support executing idempotent operations i.e. those that do not change data in the database. This way the node enables information to be extracted from the database. OrientDB has its own SQL dialect, which supports not only basic functions as any other SQL dialect, but also provides special graph traversal algorithms.
* Command node: to enable executing non-idempotent operations i.e. those that can change data in the database. Consequently this node is used to insert, update, and delete data in the database. This node has 3 modes that make the work with it pretty flexible, we will discuss these modes further in the post.
* Execution node: this nodes handles batch requests, this feature is very handy when you need to upload a large amount of data to the database. This way the user can specify the batch script or create it with the use of template.
* Function node: the node is used for calling the server functions. OrientDB supports storing user-defined functions that can be written in SQL and JavaScript. It allows the user to execute some complex operations and queries without writing a script for it every time.

