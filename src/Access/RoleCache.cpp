#include <Access/RoleCache.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/AccessControl.h>
#include <boost/container/flat_set.hpp>
#include <base/FnTraits.h>
#include <Common/logger_useful.h>

namespace DB
{

std::optional<RoleCache::RoleWithGuard> RoleCache::make_shared_rwg(RoleWithGuardWeak weak)
{
    RoleWithGuard shared;

    shared.role = weak.role.lock();
    if (shared.role)
    {
        shared.guard = weak.guard.lock();
        if (shared.guard)
        {
            return shared;
        }
    }

    return {};
}

std::optional<RoleCache::RoleWithGuardWeak> RoleCache::make_weak_rwg(RoleWithGuard shared)
{
    return RoleCache::RoleWithGuardWeak{shared.role, shared.guard};
}

template <typename FUNCTOR>
void RoleCache::collectRoles(EnabledRolesInfo & roles_info,
    boost::container::flat_set<UUID> & skip_ids,
    // Fn<std::optional<RoleWithGuard>(const UUID &)> auto && get_role_function,
    FUNCTOR && get_role_function,
    const UUID & role_id,
    bool is_current_role,
    bool with_admin_option)
{
    if (roles_info.enabled_roles.contains(role_id))
    {
        if (is_current_role)
            roles_info.current_roles.emplace(role_id);
        if (with_admin_option)
            roles_info.enabled_roles_with_admin_option.emplace(role_id);
        return;
    }

    if (skip_ids.contains(role_id))
        return;

    auto role = get_role_function(role_id);

    if (!role)
    {
        skip_ids.emplace(role_id);
        return;
    }

    roles_info.enabled_roles.emplace(role_id);
    if (is_current_role)
        roles_info.current_roles.emplace(role_id);
    if (with_admin_option)
        roles_info.enabled_roles_with_admin_option.emplace(role_id);

    roles_info.names_of_roles[role_id] = role->role->getName();
    roles_info.access.makeUnion(role->role->access);
    roles_info.settings_from_enabled_roles.merge(role->role->settings);

    for (const auto & granted_role : role->role->granted_roles.getGranted())
        collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, false);

    for (const auto & granted_role : role->role->granted_roles.getGrantedWithAdminOption())
        collectRoles(roles_info, skip_ids, get_role_function, granted_role, false, true);
}


RoleCache::RoleCache(const AccessControl & access_control_, int expiration_time)
    : access_control(access_control_), cache(expiration_time * 1000 /* 10 minutes by default*/) {}


RoleCache::~RoleCache() = default;


bool RoleCache::isRoleEnabled(const UUID & role_id)
{
    /// `mutex` is already locked.

    for (auto it = enabled_roles.begin(), e = enabled_roles.end(); it != e; ++it)
    {
        auto elem = it->second.enabled_roles_weak_ptr.lock();
        if (elem && elem->info->isRolePresent(role_id))
        {
            return true;
        }
    }
    return false;
}

std::shared_ptr<const EnabledRoles>
RoleCache::getEnabledRoles(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    std::lock_guard lock{mutex};
    EnabledRoles::Params params;
    params.current_roles.insert(roles.begin(), roles.end());
    params.current_roles_with_admin_option.insert(roles_with_admin_option.begin(), roles_with_admin_option.end());
    auto it = enabled_roles.find(params);
    if (it != enabled_roles.end())
    {
        auto from_cache = it->second.enabled_roles_weak_ptr.lock();
        if (from_cache)
            return from_cache;
        enabled_roles.erase(it);
    }

    auto res = std::shared_ptr<EnabledRoles>(new EnabledRoles(params));
    std::vector<RoleWithGuard> roles_with_guard_vector;

    collectEnabledRoles(*res, &roles_with_guard_vector, nullptr);

    ///  roles are in  res->enabled_roles

    enabled_roles.emplace(std::move(params), EnabledRolesWithGuard{res, std::move(roles_with_guard_vector)});
    return res;
}


void RoleCache::collectEnabledRoles(scope_guard * notifications)
{
    /// `mutex` is already locked.

    for (auto it = enabled_roles.begin(), e = enabled_roles.end(); it != e;)
    {
        auto elem = it->second.enabled_roles_weak_ptr.lock();
        if (!elem)
            it = enabled_roles.erase(it);
        else
        {
            collectEnabledRoles(*elem, nullptr  /* ???  */ , notifications);
            ++it;
        }
    }
}


void RoleCache::collectEnabledRoles(EnabledRoles & enabled, std::vector<RoleWithGuard> * role_with_guard_ptr, scope_guard * notifications)
{
    LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "top of collectEnabledRoles");
    /// `mutex` is already locked.

    /// Collect enabled roles. That includes the current roles, the roles granted to the current roles, and so on.
    auto new_info = std::make_shared<EnabledRolesInfo>();
    boost::container::flat_set<UUID> skip_ids;

    auto get_role_function = [this](const UUID & id) { return getRole(id); };

    for (const auto & current_role : enabled.params.current_roles)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, false);

    for (const auto & current_role : enabled.params.current_roles_with_admin_option)
        collectRoles(*new_info, skip_ids, get_role_function, current_role, true, true);

    /// Collect data from the collected roles.
    enabled.setRolesInfo(new_info, notifications);

    if (role_with_guard_ptr)
    {
        for (auto & role_uuid : enabled.info->enabled_roles)
        {
            auto it = role_catalog.find(role_uuid);
            role_with_guard_ptr->push_back(make_shared_rwg(it->second).value());  /// why lock not checked ?
        }
        for (auto & role_uuid : enabled.info->enabled_roles_with_admin_option)
        {
            auto it = role_catalog.find(role_uuid);
            role_with_guard_ptr->push_back(make_shared_rwg(it->second).value());
        }
        LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "collectEnabledRoles found out {} roles", role_with_guard_ptr->size());
    }

}


std::optional<RoleCache::RoleWithGuard> RoleCache::makeRole(const UUID & role_id)
{
    /// `mutex` is already locked.

    auto subscription = access_control.subscribeForChanges(role_id,
                                                    [this, role_id](const UUID &, const AccessEntityPtr & entity)
    {
        auto changed_role = entity ? typeid_cast<RolePtr>(entity) : nullptr;
        if (changed_role)
            roleChanged(role_id, changed_role);
        else
            roleRemoved(role_id);
    });

    auto role = access_control.tryRead<Role>(role_id);
    if (role)
    {
        LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "makeRoleEntry read role");
        // auto role_with_guard_ptr = std::make_shared<RoleWithGuard>(role_id, std::move(subscription));


        auto scope_guard_ptr = std::make_shared<scope_guard>(std::move(subscription));
        RolePtr role_ptr;


        RoleWithGuard role_with_guard{role_ptr /*RolePtr(role)*/, scope_guard_ptr /* std::move(subscription) */};

        // auto cache_value = Poco::SharedPtr<std::pair<RolePtr, scope_guard>>(
        //     new std::pair<RolePtr, scope_guard>{role, std::move(subscription)});
        cache.add(role_id, role_with_guard);
        role_catalog.insert(std::make_pair(role_id, make_weak_rwg(role_with_guard).value()));
        return role_with_guard;
    }

    return {};
}


std::optional<RoleCache::RoleWithGuard> RoleCache::getRole(const UUID & role_id)
{
    /// `mutex` is already locked.

    LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "top of getRole");
    auto role_from_cache = cache.get(role_id);
    if (role_from_cache)
        return *role_from_cache;

    return makeRole(role_id);
}


void RoleCache::roleChanged(const UUID & role_id, const RolePtr & changed_role)
{
    LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "top of roleChanged");
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};
    auto role_from_cache = cache.get(role_id);
    if (!role_from_cache)
    {
        LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "roleChanged no role in cache");
        if (!isRoleEnabled(role_id))
        {
            LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "role not found in enabled roles");
            return;
        }
        else
        {
            LOG_TRACE(&Poco::Logger::get("RoleCache ()"), "role is enabled");
            makeRole(role_id);
        }
    }
    else
    {
        role_from_cache->role = changed_role;
        cache.update(role_id, role_from_cache);
    }
    collectEnabledRoles(&notifications);
}


void RoleCache::roleRemoved(const UUID & role_id)
{
    /// Declared before `lock` to send notifications after the mutex will be unlocked.
    scope_guard notifications;

    std::lock_guard lock{mutex};
    cache.remove(role_id);
    collectEnabledRoles(&notifications);
}

}
