# OptionalSMT

**This project is a prototype. Use at your own risk**

Cast all fields in a Struct or Map Kafka message as optional. Only simple primitive types are supported, such as 
integer, float, boolean, and string. 

The message passed in must be flat, and the values will remain unchanged.

Deploy this SMT by placing the jar in a directory in the `plugin.path` loaded by Kafka Connect.
See the [Custom transformations](https://docs.confluent.io/current/connect/transforms/custom.html#custom-transform) 
docs for more details.

## Ideal Architecture

This is a prototype to support the JDBC Sink connector for a replica where constraints do not need to be exact.
When dealing with sources that under specify schema, it can be difficult to handle migrations. For example, if a
source adds a non-optional column but does not add a default, the JDBC Sink connector will crash due since it cannot 
perform the migration. This alleviates this problem by casting all fields as optional so the migration will succeed at
the cost of loosing the constraint.

**If using with JDBC Sink connector that allows migrations, this will remove existing non-null constraints on 
non-primary key columns but the primary key will remain unaffected!**
