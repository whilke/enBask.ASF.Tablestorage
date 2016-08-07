using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using enBask.ASF.Tablestorage.Shared;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using StorageTableCollection = Microsoft.ServiceFabric.Data.Collections.IReliableDictionary<string, string>;
using Microsoft.ServiceFabric.Data;

namespace enBask.ASF.Tablestorage.Service
{
    class documentIndexLookup
    {
        public string id { get; set; }
        public string partition { get; set; }
        public bool Valid { get; set; }

        public static implicit operator documentIndexLookup(JObject obj)
        {
            return new documentIndexLookup(obj);
        }

        public documentIndexLookup(JObject entity)
        {
            try
            {
                id = entity["id"].Value<string>();
                partition = entity["partition"].Value<string>();
                if (string.IsNullOrEmpty(id) ||
                  string.IsNullOrEmpty(partition))
                {
                    Valid = false;
                }
                else
                {
                    Valid = true;
                }
            }
            catch
            {
                Valid = false;
            }
        }
        public documentIndexLookup(string p, string i)
        {
            id = i;
            partition = p;
            Valid = true;
        }
    }
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class TableStorageService : StatefulService, ITableStorageService
    {
        public TableStorageService(StatefulServiceContext context)
            : base(context)
        { }

        private async Task<StorageTableCollection> GetTable(ITransaction tx, documentIndexLookup lookup)
        {
            return await StateManager.GetOrAddAsync<StorageTableCollection>(lookup.partition); 
        }

        #region ITableStorageService
        public async Task<StorageResponse> AddAsync(string document, bool returnEntity)
        {
            StorageResponse response = new StorageResponse();
            response.Code = StorageResponseCodes.Failed;
            try
            {
                //we store documents as raw json strings in the data store.
                var entity = JsonConvert.DeserializeObject<JObject>(document);

                documentIndexLookup lookup = (documentIndexLookup)entity;
                if (!lookup.Valid) return response;

                entity["etag"] = Guid.NewGuid().ToString();
                entity["timestamp"] = DateTime.UtcNow;
                document = JsonConvert.SerializeObject(entity);
              
                using (var tx = StateManager.CreateTransaction())
                {
                    var table = await GetTable(tx,lookup);
                    bool hasValue = false;
                    var oldValue = await table.AddOrUpdateAsync(tx, lookup.id, document,
                        (id, v)=>
                        {
                            //already has a value...
                            hasValue = true;
                            return v;
                        });

                    if (!hasValue)
                    {
                        //really is a new document.
                        await tx.CommitAsync();
                        if (returnEntity)
                        {
                            response.Context = document;
                        }

                        response.Code = StorageResponseCodes.Success;
                    }
                }

            }
            catch
            {

            }

            return response;
        }

        public async Task<StorageResponse> DeleteAsync(string document)
        {
            StorageResponse response = new StorageResponse();
            response.Code = StorageResponseCodes.Failed;
            try
            {
                //we store documents as raw json strings in the data store.
                var entity = JsonConvert.DeserializeObject<JObject>(document);

                documentIndexLookup lookup = (documentIndexLookup)entity;
                if (!lookup.Valid) return response;

                using (var tx = StateManager.CreateTransaction())
                {
                    var table = await GetTable(tx, lookup);
                    var r = await table.TryGetValueAsync(tx, lookup.id);
                    if (r.HasValue)
                    {
                        var currentEntity = JsonConvert.DeserializeObject<JObject>(r.Value);
                        //validate ETag
                        var oldTag = currentEntity["etag"].Value<string>();
                        var newTag = entity["etag"].Value<string>();

                        if (newTag == "*" || newTag == oldTag)
                        {
                            r = await table.TryRemoveAsync(tx, lookup.id);
                            if (r.HasValue)
                            {
                                //check if we can remove the entire collection.
                                var count = await table.GetCountAsync(tx);
                                if (count == 0)
                                {
                                    await StateManager.RemoveAsync(tx, lookup.partition);
                                }
                                await tx.CommitAsync();
                                response.Code = StorageResponseCodes.Success;
                            }
                        }
                    }
                }
            }
            catch
            {

            }

            return response;
        }

        public async Task<StorageResponse> GetAsync(string partition, string id)
        {
            StorageResponse response = new StorageResponse();
            response.Code = StorageResponseCodes.Failed;
            try
            {
                if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(partition))
                {
                    return response;
                }

                documentIndexLookup lookup = new documentIndexLookup(partition, id);

                //we store documents as raw json strings in the data store.
                using (var tx = StateManager.CreateTransaction())
                {
                    var table = await GetTable(tx,lookup);
                    var r = await table.TryGetValueAsync(tx, lookup.id);
                    if (r.HasValue)
                    {
                        response.Context = r.Value;
                        response.Code = StorageResponseCodes.Success;
                    }
                }
            }
            catch
            {

            }

            return response;
        }

        public async Task<StorageResponse> UpdateAsync(string document, bool returnEntity = false)
        {
            StorageResponse response = new StorageResponse();
            response.Code = StorageResponseCodes.Failed;
            try
            {
                //we store documents as raw json strings in the data store.
                var entity = JsonConvert.DeserializeObject<JObject>(document);

                documentIndexLookup lookup = (documentIndexLookup)entity;
                if (!lookup.Valid) return response;

                using (var tx = StateManager.CreateTransaction())
                {
                    var table = await GetTable(tx, lookup);
                    bool updated = true; //switch bit so that an add will flag success. updates only fail for a bad etag now.
                    await table.AddOrUpdateAsync(tx, lookup.id,
                    (val)=>
                    {
                        //was missing tag/stamp updates when it was an add request.
                        entity["etag"] = Guid.NewGuid().ToString();
                        entity["timestamp"] = DateTime.UtcNow;
                        document = JsonConvert.SerializeObject(entity);
                        return document;
                    },
                    (id, oldValue)=>
                    {
                        var oldEntity = JsonConvert.DeserializeObject<JObject>(oldValue);
                        var oldTag = oldEntity["etag"].Value<string>();
                        var newTag = entity["etag"].Value<string>();

                        if (newTag == "*" || oldTag == newTag)
                        {
                            //update properties
                            entity["etag"] = Guid.NewGuid().ToString();
                            entity["timestamp"] = DateTime.UtcNow;
                            document = JsonConvert.SerializeObject(entity);
                            return document;
                        }
                        else
                        {
                            updated = false;
                            return oldTag;
                        }
                    });

                    if (updated)
                    {
                        await tx.CommitAsync();
                        response.Code = StorageResponseCodes.Success;
                        response.Context = document;
                    }
                }

            }
            catch
            {

            }

            return response;
        }

        public async Task<List<StorageResponse>> SearchAsync(Dictionary<string,string> searchOptions)
        {
            List<StorageResponse> data = new List<StorageResponse>();
            //check if we can use a specific table.
            if (searchOptions.ContainsKey("partition"))
            {
                documentIndexLookup lookup = new documentIndexLookup(searchOptions["partition"], "");
                var table = await GetTable(null, lookup);
                return await searchTable(table, searchOptions);
            }
            else
            {
                CancellationToken token = new CancellationToken();
                using (var e = StateManager.GetAsyncEnumerator())
                {
                    while (await e.MoveNextAsync(token).ConfigureAwait(false))
                    {
                        var table = e.Current as StorageTableCollection;
                        if (table == null) continue;

                        var d = await searchTable(table, searchOptions);
                        data.AddRange(d);
                    }
                }
                return data;
            }

        }

        private async Task<List<StorageResponse>> searchTable(StorageTableCollection table, Dictionary<string, string> search)
        {
            List<StorageResponse> data = new List<StorageResponse>();
            using (var tx = StateManager.CreateTransaction())
            {
                var enumer = await table.CreateEnumerableAsync(tx);

                CancellationToken token = new CancellationToken();
                using (var e = enumer.GetAsyncEnumerator())
                {
                    while (await e.MoveNextAsync(token).ConfigureAwait(false))
                    {
                        var kvp = e.Current;

                        if (SearchDocument(kvp.Value, search))
                        {
                            StorageResponse response = new StorageResponse();
                            response.Code = StorageResponseCodes.Success;
                            response.Context = kvp.Value;
                            data.Add(response);
                        }

                    }
                }
            }
            return data;
        }
        private bool SearchDocument(string document, Dictionary<string,string> search)
        {
            JObject obj = JsonConvert.DeserializeObject<JObject>(document);
            foreach(var kvp in search)
            {
                var key = kvp.Key;
                if (obj[key] != null)
                {
                    var objVal = obj[key].Value<string>();
                    if (objVal != kvp.Value)
                        return false;
                }
                else
                    return false;
            }

            return true;
        }

        #endregion

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]
           {
                new ServiceReplicaListener(
                    context => new FabricTransportServiceRemotingListener(context, this))
            };

        }
    }
}
