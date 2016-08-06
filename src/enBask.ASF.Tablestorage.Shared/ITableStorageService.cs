using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enBask.ASF.Tablestorage.Shared
{
    public interface ITableStorageService : IService
    {
        Task<StorageResponse> AddAsync(string document, bool returnEntity);
        Task<StorageResponse> GetAsync(string partition, string id);
        Task<StorageResponse> DeleteAsync(string document);
        Task<StorageResponse> UpdateAsync(string document, bool returnEntity);
        Task<List<StorageResponse>> SearchAsync(Dictionary<string, string> search);
    }
}
