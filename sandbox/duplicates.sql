with
adapty_subs as ( -- собираем подписки, сделанные напрямую через приложение -- adapty
  select *
  from (
    select *, row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.smartyme_public__subscriptions
  )
  where row_num = 1
    and vendor = 'adapty'
),
profiles as (
  select id as profile_id, auth_user_id, true as is_in_profiles
  from data_bronze_supabase_prod.smartyme_public__profiles
  union
  select id as profile_id, user_id as auth_user_id, true as is_in_profiles
  from data_bronze_supabase_prod.mindscape_public__profiles
),
auth_users as (
  select
    auth_user_id,
    email,
    true as is_in_users
  from (
    select
      id as auth_user_id,
      lower(raw_user_meta_data__email) as email,
      row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.smartyme_auth__users
    union
    select
      id as auth_user_id,
      lower(raw_user_meta_data__email) as email,
      row_number() over (partition by id order by updated_at desc) as row_num
    from data_bronze_supabase_prod.mindscape_auth__users
  ) where row_num = 1
),
adapty as ( -- готовим полные данные adapty
  select
    auth_users.email,
    'SmartyMe Courses' as project_name, -- в адапти нельзя отделить буксы от курсов at the moment
    '' as web_funnel_short,
    'Adapty' as ff_sf,
    product_name as vendor_product_name,
    subs.vendor_product_id as subscription_id, -- в адапти подписки считаем по продукту,
    min(date_trunc('second', subs.created_at)) as subscription_created_at,
    max(subs.end_date) as subscription_ends_at
  from adapty_subs as subs
    left join profiles using(profile_id)
    left join auth_users using(auth_user_id)
  group by 1,2,3,4,5,6
),
paddle_temp as (
  select
    subscription_id,
    subscription_updated_at,
    current_period_end
  from (
    select
        data__id as subscription_id,
        data__status as sub_status,
        data__updated_at as subscription_updated_at,
        coalesce(data__canceled_at, data__current_billing_period__ends_at) as current_period_end,
        row_number() over (
            partition by data__id
            order by data__updated_at desc
        ) as sub_unique_row
    from firehose_paddle_webhook_prod.subscription_updated
  )
  where sub_unique_row = 1
),
ff as ( -- собираем подписки из funnelfox
  select
    lower(pts.user_email) as email,
    pts.project_name,
    pts.web_funnel_short,
    'FunnelFox' as ff_sf,
    pts.vendor_product_name,
    pts.vendor_subscription_id,
    min(pts.subscription_created_at) as subscription_created_at,
    max(
        case
            when vendor = 'paddle'
                then coalesce(paddle_temp.current_period_end, pts.current_period_end)
            else pts.current_period_end
        end
    ) as subscription_ends_at
  from data_silver_product_sessions_prod.ff_purchase_sessions pts
      left join paddle_temp
        on pts.vendor_subscription_id = paddle_temp.subscription_id
        and pts.vendor = 'paddle'
  where 1=1
    -- and not is_recurrent -- включаем продления недельных триалов
    and not is_upsell
    and purchase_completed > 0
    and product_id is not null
    and (
      vendor_subscription_id is not null
      or vendor_product_name like '%ot_lifetime_%'
    )
  group by 1,2,3,4,5,6
),
sf as ( -- собираем подписки из stellarfunnel
  select
    lower(user_email) as email,
    project_name,
    web_funnel_short,
    'StellarFunnel' as ff_sf,
    vendor_product_name,
    vendor_subscription_id,
    min(subscription_created_at) as subscription_created_at,
    max(current_period_end) as subscription_ends_at
  from data_silver_product_sessions_prod.sf_purchase_sessions
  where 1=1
    and not is_recurrent
    and not is_upsell
    and purchase_completed > 0
    and product_id is not null
    and (
      vendor_subscription_id is not null
      or vendor_product_name like '%ot_lifetime_%'
    )
  group by 1,2,3,4,5,6
),
full_union as ( -- собираем все подписки
  select
    case
        when sub.project_name in ('PROD SmartyMe', 'smartyme')
            then
                case
                    when regexp_like(sub.web_funnel_short, '^smartyme(-|.*-)m[0-9]')
                        then 'SmartyMe Books'
                    else 'SmartyMe Courses'
                end
        when project_name in ('ST PROD Mindscape App', 'StellarTech Irina')
            then 'Mindscape'
        else coalesce(sub.project_name, 'Not Attributed')
    end as project_name,
    sub.email,
    sub.ff_sf,
    sub.vendor_product_name,
    coalesce(sub.vendor_subscription_id, sub.email || cast(sub.subscription_created_at as varchar)) as vendor_subscription_id,
    sub.subscription_created_at,
    case when vendor_product_name like '%ot_lifetime_%'
      then coalesce(sub.subscription_ends_at, date_add('day', 7, sub.subscription_created_at))
      else sub.subscription_ends_at end as subscription_ends_at
  from (
    select * from ff union
    select * from sf union
    select * from adapty
  ) sub
  where not regexp_like( -- исключаем тестовые имейлы
    sub.email,
    'test|ppkostenko|stellarlab|xironikys|pikachy94|lampalampa|shelest'
  )
),
full_union_window as (-- соединяем с активными подписками на момент создания новой, составляем оконки
  select t1.*,
    t2.vendor_subscription_id as active_sub_id,
    t2.subscription_ends_at as active_sub_ends_at,
    t2.subscription_created_at as active_sub_created_at,
    t2.ff_sf as active_sub_ff_sf,
    row_number() over (
      partition by t1.email, t1.project_name, t1.subscription_created_at
      order by t2.subscription_created_at desc
    ) as active_sub_num,
    count(1) over (
      partition by t1.email, t1.project_name, t1.subscription_created_at
    ) + 1 as active_sub_cnt
  from full_union t1
  left join full_union t2
    on t1.email = t2.email
    and t1.project_name = t2.project_name
    and t1.subscription_created_at > t2.subscription_created_at
    and t1.subscription_created_at < t2.subscription_ends_at
),
duplicates as (
  select
    project_name,
    email,
    vendor_product_name,
    vendor_subscription_id,
    subscription_created_at,
    subscription_ends_at,
    ff_sf,
    active_sub_id,
    active_sub_created_at,
    active_sub_ends_at,
    active_sub_ff_sf,
    active_sub_num,
    active_sub_cnt,
    case
      when active_sub_ends_at is not null
      then
        case
          when (ff_sf = 'Adapty') or (active_sub_num in(1,2) and active_sub_ff_sf = 'Adapty')
          then 2
          else 1
        end
      else 0
    end as duplicate_flag,
    case
      max(
        case
          when active_sub_ends_at is not null
          then
            case
              when (ff_sf = 'Adapty') or (active_sub_num in(1,2) and active_sub_ff_sf = 'Adapty')
              then 2
              else 1
            end
          else 0
        end
      ) over (partition by email, project_name, subscription_created_at)
      when 2 then 'Mobile duplicate'
      when 1 then 'Web duplicate'
      else null
    end as duplicate
  from full_union_window
  where 1=1
    and date(subscription_created_at) between date('2025-12-22') and date('2025-12-28')
    and active_sub_ends_at is not null
)
select
  project_name,
  email,
  vendor_product_name,
  vendor_subscription_id,
  subscription_created_at,
  subscription_ends_at,
  ff_sf,
  duplicate,
  active_sub_cnt,
  array_agg(
    active_sub_ff_sf
    || ' sub_id '
    || active_sub_id
    || ': '
    || cast(date(active_sub_created_at) as varchar)
    || ' to '
    || cast(date(active_sub_ends_at) as varchar)
    order by active_sub_created_at
  ) as active_subs_info
from duplicates
group by 1,2,3,4,5,6,7,8,9
order by subscription_created_at desc