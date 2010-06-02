using System;

namespace Raven.ManagedStorage.DataRecords
{
    [Flags]
    public enum RecordType
    {
        Put = 0x1,
        IndexPut = 0x2,
        Delete = 0x4,
        IndexDelete = 0x8,
        Invalidated = 0x1B0
    }
}