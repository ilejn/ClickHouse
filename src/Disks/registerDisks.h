#pragma once

#include <Core/Types.h>

namespace DB
{

enum class DiskFlag : size_t
{
    /// skip access check regardless .skip_access_check config directive (used for clickhouse-disks)
    GLOBAL_SKIP_ACCESS_CHECK = 0,

    /// ObjectStorageVFS is allowed (used e.g. for Keeper)
    ALLOW_VFS,

    /// ObjectStorageVFS Garbage Collector is allowed (used for clickhouse-disks)
    ALLOW_VFS_GC,
};

/// just a syntax sugar to use scoped enum with bitset without explicit cast
template <typename FLAG, typename BODY>
class FlagsSet : public BODY
{
    using SelfType = DB::FlagsSet<FLAG, BODY>;

public:
    constexpr bool test(FLAG flag) { return BODY::test(static_cast<size_t>(flag)); }
    constexpr SelfType & set(FLAG pos, bool value = true) { BODY::set(static_cast<size_t>(pos), value); return *this;}
    constexpr SelfType & flip(FLAG pos) { BODY::flip(static_cast<size_t>(pos)); return *this;}
    constexpr bool operator[](FLAG pos) const { return BODY::operator[](static_cast<size_t>(pos));}
};

using DiskFlagsBody = std::bitset<3>;
using DiskFlags = FlagsSet<DiskFlag, DiskFlagsBody>;

void registerDisks(DiskFlags disk_flags);
}
