with ranked_clicks as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() over (
            partition by s.visitor_id
            order by s.visit_date desc
        ) as rn
    from sessions as s
    left join
        leads as l
        on s.visitor_id = l.visitor_id and s.visit_date <= l.created_at
    where s.medium != 'organic'
),

spendings as (
    select
        DATE(campaign_date) as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) as total_cost
    from vk_ads
    group by 1, 2, 3, 4
    union
    select
        DATE(campaign_date) as campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) as total_cost
    from ya_ads
    group by 1, 2, 3, 4
),

agg_tab as (
    select
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(visit_date) as visit_date,
        COUNT(visitor_id) as visitors_count,
        COUNT(lead_id) as leads_count,
        COUNT(lead_id) filter (
            where status_id = 142
        ) as purchases_count,
        SUM(amount) as revenue
    from ranked_clicks
    where rn = 1
    group by 4, 1, 2, 3
    order by
        8 desc nulls last, 4, 5 desc, 1 asc, 2 asc, 3 asc
)

select
    agg_tab.visit_date,
    agg_tab.visitors_count,
    agg_tab.utm_source,
    agg_tab.utm_medium,
    agg_tab.utm_campaign,
    sp.total_cost,
    agg_tab.leads_count,
    agg_tab.purchases_count,
    agg_tab.revenue
from agg_tab
left join spendings as sp
    on
        agg_tab.utm_source = sp.utm_source
        and agg_tab.utm_medium = sp.utm_medium
        and agg_tab.utm_campaign = sp.utm_campaign
        and agg_tab.visit_date = sp.campaign_date
order by
    9 desc nulls last, 1, 2 desc, 3 asc, 4 asc, 5 asc;
