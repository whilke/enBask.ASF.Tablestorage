using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enBask.ASF.Tablestorage.Shared
{
    public enum StorageResponseCodes
    {
        Invalid = 0,
        Success,
        Failed
    }
    public class StorageResponse
    {
        public StorageResponseCodes Code { get; set; }
        public string Context { get; set; }
        internal StorageResponse(StorageResponseCodes code)
        {
            Code = code;
        }

        public StorageResponse()
        {

        }
    }
    
    public class TableResponse<T> where T : ITableEntity
    {
        public T Context { get; set; }
        public StorageResponseCodes Response { get; set; }
    }
}
