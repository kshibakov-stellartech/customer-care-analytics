with
scopes as ( -- собираем scopes по подпискам
  select *
  from (
    select
        'SmartyMe' as project_name,
        a.id as subscription_id,
        a.created_at as subscription_created_at,
        a.updated_at as subscription_updated_at,
        a.product_name,
        a.profile_id,
        a.vendor as vendor_name,
        b.value as features_scope,
        row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.smartyme_public__subscriptions a
      left join data_bronze_supabase_prod.smartyme_public__subscriptions__subscription_features_scope b
      on a.id = b._parent_id
    where b.value in ('basic','free')
    union
    select
        'Mindscape' as project_name,
        a.id as subscription_id,
        a.created_at as subscription_created_at,
        a.updated_at as subscription_updated_at,
        a.product_name,
        a.profile_id,
        a.vendor as vendor_name,
        b.value as features_scope,
        row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.mindscape_public__subscriptions a
      left join data_bronze_supabase_prod.mindscape_public__subscriptions__subscription_features_scope b
      on a.id = b._parent_id
    where b.value in ('basic','free')
  )
  where row_num = 1
),
adapty_scope as ( -- есть ли доступ через адапти
  select project_name, profile_id, true as adapty_basic
  from scopes
  where vendor_name = 'adapty'
    and features_scope = 'basic'
  group by 1, 2, 3
),
profiles as (
  select
    'SmartyMe' as project_name,
    id as profile_id,
    auth_user_id,
    customer_id,
    ff_profile_id,
    true as is_in_profiles
  from data_bronze_supabase_prod.smartyme_public__profiles
  union
  select
    'Mindscape' as project_name,
    id as profile_id,
    user_id as auth_user_id,
    customer_id,
    ff_profile_id,
    true as is_in_profiles
  from data_bronze_supabase_prod.mindscape_public__profiles
),
auth_users as (
  select
    project_name,
    auth_user_id,
    email,
    true as is_in_users
  from (
    select
      'SmartyMe' as project_name,
      id as auth_user_id,
      lower(raw_user_meta_data__email) as email,
      row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.smartyme_auth__users
    union
    select
      'Mindscape' as project_name,
      id as auth_user_id,
      lower(raw_user_meta_data__email) as email,
      row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.mindscape_auth__users
  ) where row_num = 1
),
potential_customers as (
  -- select
  --   'Mindscape' as project_name,
  --   ff_profile_id,
  --   ff_email
  -- from data_bronze_supabase_prod.mindscape_crm_potential_customers
  -- union
  select
    'Mindscape' as project_name,
    ff_profile_id,
    ff_email
  from data_bronze_supabase_prod.mindscape_crm__potential_customers
  union
  -- select
  --   'SmartyMe' as project_name,
  --   ff_profile_id,
  --   ff_email
  -- from data_bronze_supabase_prod.smartyme_crm_potential_customers
  -- union
  select
    'SmartyMe' as project_name,
    ff_profile_id,
    ff_email
  from data_bronze_supabase_prod.smartyme_crm__potential_customers
),
purchases as (
    select
      'SmartyMe' as project_name,
      vendor_profile_id as vendor_customer_id,
      -- profile_id,
      subscription_id,
      created_at as charged_at,
      session_id,
      refunded_at
    from data_bronze_supabase_prod.smartyme_public__purchases
    union
    select
      'Mindscape' as project_name,
      vendor_profile_id as vendor_customer_id,
      -- profile_id,
      subscription_id,
      created_at,
      session_id,
      refunded_at
    from data_bronze_supabase_prod.mindscape_public__purchases
),
cs as ( -- customer_support_new
  select
    coalesce(
      purchases.project_name,
      scopes.project_name,
      profiles.project_name,
      auth_users.project_name
    ) as project_name,
    profiles.is_in_profiles,
    if(
      not profiles.is_in_profiles
        or profiles.is_in_profiles is null,
      'platform_user',
      null
    ) as platform_user,
    auth_users.is_in_users,
    if(auth_users.is_in_users,true,false) as registration_finished,
    lower(potential_customers.ff_email) as contact_email,
    profiles.customer_id,
    profiles.auth_user_id AS auth_id,
    scopes.product_name,
    coalesce(
      scopes.features_scope,
      if(
        not profiles.is_in_profiles
          or profiles.is_in_profiles is null,
        'platform_user',
        null
      )
    ) as features_scope,
    scopes.vendor_name,
    lower(auth_users.email) as auth_email,
    lower(coalesce(potential_customers.ff_email,auth_users.email)) as email_combined,
    purchases.vendor_customer_id,
    purchases.session_id,
    scopes.subscription_id,
    purchases.charged_at,
    purchases.refunded_at,
    coalesce(adapty_scope.adapty_basic, false) as adapty_basic
  from purchases

    full outer join scopes
      on purchases.project_name = scopes.project_name
      and purchases.subscription_id = scopes.subscription_id

    full outer join profiles
      on scopes.project_name = profiles.project_name
      and scopes.profile_id = profiles.profile_id

    full outer join auth_users
      on profiles.project_name = auth_users.project_name
      and profiles.auth_user_id = auth_users.auth_user_id

    left join potential_customers
      on profiles.project_name = potential_customers.project_name
      and profiles.ff_profile_id = potential_customers.ff_profile_id

    left join adapty_scope
      on coalesce(
        purchases.project_name,
        scopes.project_name,
        profiles.project_name,
        auth_users.project_name
      ) = adapty_scope.project_name
      and scopes.profile_id = adapty_scope.profile_id
),
cs_agg as (
  select
    *,
    case
      when contains(features_scope, 'free') and not contains(features_scope, 'basic') then 'free only'
      when contains(features_scope, 'free') and contains(features_scope, 'basic') then 'basic + free'
      when not contains(features_scope, 'free') and contains(features_scope, 'basic') then 'basic'
      when contains(features_scope, 'platform_user') then 'platform_user'
      when contains(features_scope, null) then 'no record'
      when features_scope is null then 'no record'
      else array_join(features_scope, ', ')
    end as feature_label
  from (
    select
      project_name,
      email_combined as email,
      registration_finished,
      array_distinct(array_agg(features_scope)) as features_scope
    from cs
    group by 1, 2, 3
  )
),
cs_agg_regtrue as (
  select
    project_name,
    email,
    feature_label as scopes_regtrue
  from cs_agg
  where registration_finished
),
cs_agg_regfalse as (
  select
    project_name,
    email,
    feature_label as scopes_regfalse
  from cs_agg
  where not registration_finished
),
cs_agg_adapty as (
  select
    project_name,
    email_combined as email,
    adapty_basic
  from cs
  where adapty_basic
  group by 1, 2, 3
),
new_subs as ( -- собираем подписки за нужный период
  select
    lower(user_email) as email,
    case
      when project_name in ('PROD SmartyMe', 'smartyme') then 'SmartyMe'
      when project_name in ('ST PROD Mindscape App', 'StellarTech Irina') then 'Mindscape'
    end as project_name,
    web_funnel_short,
    'FunnelFox' as ff_sf,
    vendor,
    vendor_product_name,
    product_id,
    vendor_customer_id,
    vendor_subscription_id,
    purchase_completed_at,
    subscription_canceled_at,
    charge_refunded_at
  from data_silver_product_sessions_prod.ff_purchase_sessions
  where 1=1
    and not is_recurrent
    and not is_upsell
    and user_email is not null
    and purchase_completed > 0
    and vendor_product_name is not null
    and (
      vendor_subscription_id is not null
      or vendor_product_name like '%ot_lifetime_%' -- включаем недельный триал с продлением
    )
    and date(purchase_completed_at) between date('2025-12-29') and date('2026-01-04')
  union
  select
    lower(user_email) as email,
    case
      when project_name in ('PROD SmartyMe', 'smartyme') then 'SmartyMe'
      when project_name in ('ST PROD Mindscape App', 'StellarTech Irina') then 'Mindscape'
    end as project_name,
    web_funnel_short,
    'StellarFunnel' as ff_sf,
    vendor,
    vendor_product_name,
    product_id,
    vendor_customer_id,
    vendor_subscription_id,
    purchase_completed_at,
    subscription_canceled_at,
    charge_refunded_at
  from data_silver_product_sessions_prod.sf_purchase_sessions
  where 1=1
    and not is_recurrent
    and not is_upsell
    and user_email is not null
    and purchase_completed > 0
    and vendor_product_name is not null
    and (
      vendor_subscription_id is not null
      or vendor_product_name like '%ot_lifetime_%'-- включаем недельный триал с продлением
    )
    and date(purchase_completed_at) between date('2025-12-29') and date('2026-01-04')
)
select
  email,
  ff_sf,
  project_name,
  web_funnel_short,
  vendor,
  vendor_product_name,
  product_id,
  vendor_customer_id,
  vendor_subscription_id,
  purchase_completed_at,
  subscription_canceled_at,
  charge_refunded_at,
  coalesce(adapty_basic, false) as adapty_basic,
  coalesce(scopes_regtrue, 'no records') as scopes_regtrue,
  coalesce(scopes_regfalse, 'no records') as scopes_regfalse
from new_subs
left join cs_agg_regtrue
  using (project_name, email)
left join cs_agg_regfalse
  using (project_name, email)
left join cs_agg_adapty
  using (project_name, email)
order by purchase_completed_at desc