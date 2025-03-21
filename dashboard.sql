--количество уникальных посетителей и лидов по датам и ресурсам
SELECT
    s.visit_date::date AS visit_date,
    s.source,
    COUNT(DISTINCT s.visitor_id) AS distinct_visitors_count,
    COUNT(DISTINCT l.lead_id) AS leads_count
FROM sessions AS s
LEFT JOIN
    leads AS l
    ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
GROUP BY 1, 2
ORDER BY 1;

--конверсия из клика в лид
WITH tbl AS (
    SELECT
        s.visit_date::date AS visit_date,
        COUNT(DISTINCT s.visitor_id) AS distinct_visitors_count,
        COUNT(DISTINCT l.lead_id) AS leads_count
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    GROUP BY 1
    ORDER BY 1
)

SELECT
    visit_date,
    SUM(leads_count) * 100.0 / SUM(distinct_visitors_count) AS lcr
FROM tbl
GROUP BY 1;

--конверсия из лида в оплату (подробно)
WITH tbl AS (
    SELECT
        s.source,
        s.medium,
        s.campaign,
        COUNT(l.lead_id) AS leads_count,
        COUNT(l.lead_id) FILTER
        (WHERE l.status_id = 142)
        AS purchase_count
    FROM leads AS l
    LEFT JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
    WHERE s.medium != 'organic'
    GROUP BY 1, 2, 3
)

SELECT
    source,
    medium,
    campaign,
    leads_count,
    purchase_count,
    ROUND(100.0 * purchase_count / leads_count, 2) AS cr
FROM tbl
ORDER BY 6 DESC;

--конверсия из лида в оплату (кратко)
WITH tbl AS (
    SELECT
        s.source,
        COUNT(l.lead_id) AS leads_count,
        COUNT(l.lead_id) FILTER
        (WHERE l.status_id = 142)
        AS purchase_count
    FROM leads AS l
    LEFT JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
    WHERE s.medium != 'organic'
    GROUP BY 1
)

SELECT
    source,
    leads_count,
    purchase_count,
    ROUND(100.0 * purchase_count / leads_count, 2) AS cr
FROM tbl
ORDER BY 4 DESC;

--CPU, CLP, CPPU, ROI
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
    where s.medium <> 'organic'
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
),

tab as (
    select
        agg_tab.visit_date,
        agg_tab.utm_source,
        agg_tab.utm_medium,
        agg_tab.utm_campaign,
        agg_tab.visitors_count,
        sp.total_cost,
        agg_tab.leads_count,
        agg_tab.purchases_count,
        agg_tab.revenue
    from agg_tab
    inner join spendings as sp
        on
            agg_tab.utm_source = spendings.utm_source
            and agg_tab.utm_medium = spendings.utm_medium
            and agg_tab.utm_campaign = spendings.utm_campaign
            and agg_tab.visit_date = spendings.campaign_date
    order by 9 desc nulls last, 1, 5 desc, 2, 3, 4
)

select
    utm_source,
    case
        when SUM(visitors_count) = 0 then 0
        else ROUND(SUM(total_cost) / SUM(visitors_count), 2)
    end as cpu,
    case
        when SUM(leads_count) = 0 then 0
        else ROUND(SUM(total_cost) / SUM(leads_count), 2)
    end as cpl,
    case
        when SUM(purchases_count) = 0 then 0
        else ROUND(SUM(total_cost) / SUM(purchases_count), 2)
    end as cppu,
    ROUND(
        100.0 * (SUM(revenue) - SUM(total_cost)) / SUM(total_cost), 2
    ) as roi
from tab
group by 1;

-- Затраты
select
	campaign_date::date,
	utm_source,
	utm_medium,
	utm_campaign,
	sum(daily_spent) as total_cost
from vk_ads
group by 1, 2, 3, 4
order by 1
union 
select
	campaign_date::date,
	utm_source,
	utm_medium,
	utm_campaign,
	sum(daily_spent) as total_cost
from ya_ads
group by 1, 2, 3, 4
order by 1;