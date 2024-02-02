#include "registerDisks.h"

#include "DiskFactory.h"

#include "config.h"

namespace DB
{

void registerDiskLocal(DiskFactory & factory, DiskFlags disk_flags);

#if USE_SSL
void registerDiskEncrypted(DiskFactory & factory, DiskFlags disk_flags);
#endif

void registerDiskCache(DiskFactory & factory, DiskFlags disk_flags);
void registerDiskObjectStorage(DiskFactory & factory, DiskFlags disk_flags);


#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD

void registerDisks(DiskFlags disk_flags)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, disk_flags);

#if USE_SSL
    registerDiskEncrypted(factory, disk_flags);
#endif

    registerDiskCache(factory, disk_flags);

    registerDiskObjectStorage(factory, disk_flags);
}

#else

void registerDisks(DiskFlags disk_flags)
{
    auto & factory = DiskFactory::instance();

    registerDiskLocal(factory, disk_flags);

    registerDiskObjectStorage(factory, disk_flags);
}

#endif

}
