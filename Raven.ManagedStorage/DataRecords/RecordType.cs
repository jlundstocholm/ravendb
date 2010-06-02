using System;

namespace Raven.ManagedStorage.DataRecords
{
    [Flags]
    public enum RecordType
    {
        Put = 0x1,
        Checkpoint = 0x2,
        IndexPut = 0x4,
        Delete = 0x8,
        IndexDelete = 0x10
    }
}