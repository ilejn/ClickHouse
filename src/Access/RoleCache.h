#pragma once
#include <Access/EnabledRoles.h>
#include <Poco/AccessExpireCache.h>
#include <boost/container/flat_set.hpp>
#include <map>
#include <mutex>
// #include <base/FnTraits.h>  // get rid of it


namespace DB
{
class AccessControl;
struct Role;
using RolePtr = std::shared_ptr<const Role>;

class RoleCache
{
public:
    explicit RoleCache(const AccessControl & access_control_, int expiration_time);
    ~RoleCache();

    std::shared_ptr<const EnabledRoles> getEnabledRoles(
        const std::vector<UUID> & current_roles,
        const std::vector<UUID> & current_roles_with_admin_option);

private:
    using scope_guard_ptr_t = std::shared_ptr<scope_guard>;
    using scope_guard_ptr_weak_t = std::weak_ptr<scope_guard>;
    using RoleWeakPtr = std::weak_ptr<const DB::Role>;

    struct RoleWithGuard
    {
        RolePtr role;
        scope_guard_ptr_t guard;
    };

    struct RoleWithGuardWeak
    {
        RoleWeakPtr role;
        scope_guard_ptr_weak_t guard;
    };

    std::optional<RoleWithGuard> make_shared_rwg(RoleWithGuardWeak);
    std::optional<RoleWithGuardWeak> make_weak_rwg(RoleWithGuard shared);


    // using RoleWithGuardPtr = std::shared_ptr<RoleWithGuard>;
    // using RoleWithGuardWeakPtr = std::weak_ptr<RoleWithGuard>;

    struct EnabledRolesWithGuard
    {
        std::weak_ptr<EnabledRoles>  enabled_roles_weak_ptr;
        std::vector<RoleWithGuard> roles_with_guard_ptr;
    };

    void collectEnabledRoles(scope_guard * notifications);
    void collectEnabledRoles(EnabledRoles & enabled,
        std::vector<RoleWithGuard> * role_with_guard_ptr,
        scope_guard * notifications);
    std::optional<RoleWithGuard> getRole(const UUID & role_id);
    std::optional<RoleWithGuard> makeRole(const UUID & role_id);

    void roleChanged(const UUID & role_id, const RolePtr & changed_role);
    void roleRemoved(const UUID & role_id);

    bool isRoleEnabled(const UUID & role_id);
    template <typename FUNCTOR>
    void collectRoles(EnabledRolesInfo & roles_info,
        boost::container::flat_set<UUID> & skip_ids,
        // Fn<std::optional<RoleWithGuard>(const UUID &)> auto && get_role_function,
        FUNCTOR && get_role_function,
        const UUID & role_id,
        bool is_current_role,
        bool with_admin_option);


    const AccessControl & access_control;

    Poco::AccessExpireCache<UUID, RoleWithGuard> cache;
    std::map<EnabledRoles::Params, EnabledRolesWithGuard> enabled_roles;  /// deleted by joine'd scope_guard

    /// To access a role regardless if it is in cache or not
    /// Items are erased by scope_guard
    ///   upon deleting both from cache and enabled_roles
    std::map<UUID, RoleWithGuardWeak> role_catalog;   /// switch to unordered_set


    mutable std::mutex mutex;
};

}
