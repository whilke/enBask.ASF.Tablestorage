using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace enBask.ASF.Tablestorage
{
    public interface ITableEntity
    {
        string Partition { get; set; }
        string Id { get; set; }
        string ETag { get; set; }
        DateTime Timestamp { get; set; }
    }
}
