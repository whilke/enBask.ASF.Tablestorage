using enBask.ASF.Tablestorage.Shared;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enBask.ASF.Tablestorage.Client
{
    public class TableClient
    {
        const string service_uri = "fabric:/enBask.ASF.Tablestorage.App/TableService-{0}";
        private Uri app_uri = new Uri("fabric:/enBask.ASF.Tablestorage.App");
        private Uri table_uri;

        private string _tableName;
        private int partitionCount;

        long Hash(string key)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(key);
            const ulong fnv64Offset = 14695981039346656037;
            const ulong fnv64Prime = 0x100000001b3;
            ulong hash = fnv64Offset;

            for (var i = 0; i < bytes.Length; i++)
            {
                hash = hash ^ bytes[i];
                hash *= fnv64Prime;
            }

            return (long)hash;
        }

        public TableClient(string tableName, int instanceCount)
        {
            _tableName = tableName;
            partitionCount = instanceCount;

            table_uri = new Uri(string.Format(service_uri, _tableName));
        }
        public async Task CreateIfNotExsistAsync()
        {
            FabricClient client = new FabricClient();

            var services = await client.QueryManager.GetServiceListAsync(app_uri, table_uri);
            if (services == null || services.Count == 0)
            {
                //service must be created.
                StatefulServiceDescription sd = new StatefulServiceDescription();
                sd.ApplicationName = app_uri;
                sd.HasPersistedState = true;
                sd.MinReplicaSetSize = 2;
                sd.TargetReplicaSetSize = 3;
                sd.ServiceTypeName = "enBask.Tablestorage.ServiceType";
                sd.ServiceName = table_uri;
                sd.PartitionSchemeDescription = 
                    new UniformInt64RangePartitionSchemeDescription(partitionCount, -9223372036854775808, 9223372036854775807);

                await client.ServiceManager.CreateServiceAsync(sd);
            }
        }
        public async Task Delete()
        {
            FabricClient client = new FabricClient();

            var services = await client.QueryManager.GetServiceListAsync(app_uri, table_uri);
            foreach (var service in services)
            {
                await client.ServiceManager.DeleteServiceAsync(service.ServiceName);
            }
        }

        public async Task<TableResponse<T>> AddAsync<T>(ITableEntity entity, bool returnEntity=false) where T : ITableEntity
        {
            TableResponse<T> response = new TableResponse<T>();
            response.Response = StorageResponseCodes.Failed;
            try
            {
                var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                    new ServicePartitionKey(Hash(entity.Partition)));

                var document = JsonConvert.SerializeObject(entity);
                var r = await proxy.AddAsync(document, returnEntity);

                if (r.Code == StorageResponseCodes.Success)
                {
                    if (returnEntity)
                    {
                        response.Context = JsonConvert.DeserializeObject<T>(r.Context);
                    }
                    response.Response = StorageResponseCodes.Success;
                }
            }
            catch
            {

            }

            return response;
        }
        public async Task<TableResponse<T>> DeleteAsync<T>(ITableEntity entity) where T : ITableEntity
        {
            TableResponse<T> response = new TableResponse<T>();
            response.Response = StorageResponseCodes.Failed;
            try
            {
                var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                    new ServicePartitionKey(Hash(entity.Partition)));

                var document = JsonConvert.SerializeObject(entity);
                var r = await proxy.DeleteAsync(document);

                if (r.Code == StorageResponseCodes.Success)
                {
                    response.Response = StorageResponseCodes.Success;
                }
            }
            catch
            {

            }

            return response;
        }
        public async Task<TableResponse<T>> UpdateAsync<T>(ITableEntity entity, bool returnEntity = false) where T : ITableEntity
        {
            TableResponse<T> response = new TableResponse<T>();
            response.Response = StorageResponseCodes.Failed;
            try
            {
                var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                    new ServicePartitionKey(Hash(entity.Partition)));

                var document = JsonConvert.SerializeObject(entity);
                var r = await proxy.UpdateAsync(document, returnEntity);

                if (r.Code == StorageResponseCodes.Success)
                {
                    if (returnEntity)
                    {
                        response.Context = JsonConvert.DeserializeObject<T>(r.Context);
                    }
                    response.Response = StorageResponseCodes.Success;
                }
            }
            catch
            {

            }

            return response;
        }
        public async Task<TableResponse<T>> GetAsync<T>(string partition, string id) where T : ITableEntity
        {
            TableResponse<T> response = new TableResponse<T>();
            response.Response = StorageResponseCodes.Failed;
            try
            {
                var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                    new ServicePartitionKey(Hash(partition)));

                var r = await proxy.GetAsync(partition, id);

                if (r.Code == StorageResponseCodes.Success)
                {
                    response.Context = JsonConvert.DeserializeObject<T>(r.Context);
                    response.Response = StorageResponseCodes.Success;
                }
            }
            catch
            {

            }

            return response;
        }
        public async Task<IEnumerable<TableResponse<T>>> SearchAsnyc<T>(Dictionary<string,string> searchParams) where T: ITableEntity
        {
            List<TableResponse<T>> data = new List<TableResponse<T>>();
            //first check if this search has a partition restriction
            if (searchParams.ContainsKey("partition"))
            {
                //send the search to one partition.
                var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                   new ServicePartitionKey(Hash(searchParams["partition"])));

                var results = await _partition_search<T>(proxy, searchParams);
                data.AddRange(results);
            }
            else
            {
                //send it to all partitions.
                FabricClient client = new FabricClient();
                var partitions = await client.QueryManager.GetPartitionListAsync(table_uri);

                var r = Parallel.ForEach(partitions, new ParallelOptions() { MaxDegreeOfParallelism = 5 },
                    (p) =>
                    {
                        Int64RangePartitionInformation info = p.PartitionInformation as Int64RangePartitionInformation;
                        var key = info.LowKey;
                        var proxy = ServiceProxy.Create<ITableStorageService>(table_uri,
                            new ServicePartitionKey(key));
                        var results = _partition_search<T>(proxy, searchParams).Result;
                        lock(data)
                        {
                            data.AddRange(results);
                        }
                    });


            }

            return data;
        }

        private async Task<IEnumerable<TableResponse<T>>> _partition_search<T>(ITableStorageService service, Dictionary<string,string> searchParams) where T : ITableEntity
        {
            var data = await service.SearchAsync(searchParams);
            return data.Select<StorageResponse, TableResponse<T>>((sr) =>
           {
               TableResponse<T> tr = new TableResponse<T>();
               tr.Response = sr.Code;
               if (tr.Response == StorageResponseCodes.Success)
                   tr.Context = JsonConvert.DeserializeObject<T>(sr.Context);
               return tr;
           });
        }
    }
}
