#pragma once

#include <Core/Types.h>

namespace DB
{

enum class DiskFlag : size_t
{
    /// skip access check regardless regardless .skip_access_check config directive (used for clickhouse-disks)
    GLOBAL_SKIP_ACCESS_CHECK = 0,
    ALLOW_VFS,
    ALLOW_VFS_GC,
};

using FlagsBody = std::bitset<4>;
class DiskFlags : public FlagsBody
{
public:
    constexpr bool test(DiskFlag flag) { return FlagsBody::test(static_cast<size_t>(flag)); }
    constexpr DiskFlags& set(DiskFlag pos, bool value = true) { FlagsBody::set(static_cast<size_t>(pos), value); return *this;}
    constexpr DiskFlags& flip(DiskFlag pos) {FlagsBody::flip(static_cast<size_t>(pos)); return *this;}
    constexpr bool operator[](DiskFlag pos) const { return FlagsBody::operator[](static_cast<size_t>(pos));}
};

void registerDisks(DiskFlags disk_flags);

}
