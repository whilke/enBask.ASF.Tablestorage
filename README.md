# enBask.ASF.Tablestorage
Service Fabric implementation for a document table storage system

#Install
Download the latest release ASF package and deploy into your Service Fabric cluster

#Usage
Install the client nuget package into any project that will communicate with the table storage services

```Install-Package enBask.ASF.Tablestorage```

## Data partitioning
Data is split across three keys. 
1.) Table name - Data can only belong to one table so this is your highest level grouping constraint
2.) Partition name - Each document has a partition property which will allow you to group data into the same container for quicker searches
3.) Document Id - Each document has an id property which allows the fastest lookups to individual documents.

## Consistancy
Tables use an optimistic concurrency strategy to guarantee you are not losing data without explicitly knowing about it.
This is done via an etag property attached to every document in the system. If you update or delete a document you must send a document 
with at least this property set to the value matching in the table store. You can override this behavior and utilize a last writer wins
pattern by setting the etag property to "*"

## Query performance
Queries are listed here in order of performance based on which keys you have

* Table - Partition - Id : This results in a single hashtable lookup in a specific container. The number of table instances will not affect this query
* Table - Partition : This will be a table scale against a single partition container. Depending on number of search fields and records in the container will determin performance
* Table : This is a table scan against multiple partition containers. Up to 5 table instances are searched in parallel, but all instances are searched. 
This type of search could actually be faster then knowing the partition depending on how sparse your data in partitions are and now many instances you have.

## Create a new table
A table is a unique container for non-structured data. It requires a unique table name and a total instance count.
Note: choose an instance count that will eventually scale for your data needs. 
More instances will give you better performance around your partition key but could result in slower queries across partitions.
You can not change the instance count once you create a table. You woul have to destroy the table and re-create it (losing any data).
Most uses should consider an instance count between 1 and 5. For very large clusters and datasets you can try scaling between 10 and 20.

```cs
TableClient client = new TableClient("users", 5);
```

## Create an entity type
```cs
public UserRecord(string username, string firstname, string email)
{
    Username = username;
    Firstname = firstname;
    Email = email;
    Partition = Username.Substring(0, 3);
    Id = Username;
}
```

## Add a new record
```cs
UserRecord user = new UserRecord("testuser", "joe", "joe@email.cc");
TableResponse<UserRecord> result = await client.AddAsync<UserRecord>(user, true);
```

## Find a record
```cs
//slow search across all instances of the table
Dictionary<string, string> searchParams = new Dictionary<string, string>();
searchParams["email"] = "joe@email.cc";
var results  = await client.SearchAsnyc<UserRecord>(searchParams);
```

## Delete a record
```cs
UserRecord user = new UserRecord("testuser", "joe", "joe@email.cc");
use.etag = "*" //we don't care if updates come in before the delete, we just want to force delete
var result = await client.DeleteAsync<UserRecord>(user);

```
