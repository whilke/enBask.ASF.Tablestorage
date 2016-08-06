using Newtonsoft.Json;
using System;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("enBask.ASF.Tablestorage.Service")]
[assembly: InternalsVisibleTo("enBask.ASF.Tablestorage.Client")]

namespace enBask.ASF.Tablestorage.Shared
{
    public class BaseEntity : ITableEntity
    {
        #region ITableEntity
        [JsonProperty("partition")]
        public string Partition
        {
            get;set;
        }
        [JsonProperty("id")]
        public string Id
        {
            get; set;
        }
        [JsonProperty("etag")]
        public string ETag
        {
            get; set;
        }
        [JsonProperty("timestamp")]
        public DateTime Timestamp
        {
            get; set;

        }
        #endregion

        public BaseEntity()
        {

        }
    }
}
