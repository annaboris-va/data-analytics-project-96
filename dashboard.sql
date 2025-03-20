--количество уникальных посетителей и лидов по датам и ресурсам
SELECT
    s.visit_date::date AS visit_date,
    source,
    COUNT(DISTINCT s.visitor_id) AS distinct_visitors_count,
    COUNT(distinct (case 
		when visit_date <= created_at then l.lead_id
	else NULL
	end)) AS leads_count
FROM sessions AS s
LEFT JOIN leads AS l
    using (visitor_id)
GROUP BY 1,2
ORDER BY 1;

--конверсия из клика в лид
WITH tbl AS (
    SELECT
        s.visit_date::date AS visit_date,
        COUNT(DISTINCT s.visitor_id) AS distinct_visitors_count,
        COUNT(distinct (case 
		when visit_date <= created_at then l.lead_id
	else NULL
	end)) AS leads_count
    FROM sessions AS s
    LEFT JOIN leads AS l
        using (visitor_id)
    GROUP BY 1
    ORDER BY 1
)

select
	visit_date,
    SUM(leads_count)*100.0/ SUM(distinct_visitors_count) AS lcr
FROM tbl
group by 1;


--конверсия из лида в оплату (подробно)
WITH tbl AS (
    select
        s.source,
        s.medium,
        s.campaign,
        COUNT(l.lead_id) AS leads_count,
        COUNT(l.lead_id) FILTER
        (WHERE l.status_id = 142)
        AS purchase_count
    FROM leads AS l
    LEFT JOIN sessions AS s
        using(visitor_id)
    WHERE s.medium != 'organic'
    GROUP BY 1,2,3
)

select
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
    select
        s.source,
        COUNT(l.lead_id) AS leads_count,
        COUNT(l.lead_id) FILTER
        (WHERE l.status_id = 142)
        AS purchase_count
    FROM leads AS l
    LEFT JOIN sessions AS s
        using(visitor_id)
    WHERE s.medium != 'organic'
    GROUP BY 1
)

select
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
	visit_date,
	source as utm_source,
	medium as utm_medium, 
	campaign as utm_campaign,
	lead_id,
	amount,
	closing_reason,
	status_id,
	ROW_NUMBER() OVER (PARTITION BY s.visitor_id 
            ORDER BY visit_date DESC
        ) AS rn
from sessions s
left join leads l ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
where medium<>'organic'
),
spendings as (
		select 
		date(campaign_date) as campaign_date, 
		utm_source, 
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_cost
	from vk_ads 
	group by 1,2,3,4
union 
	select 
		date(campaign_date) as campaign_date, 
		utm_source, 
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_cost
	from ya_ads 
	group by 1,2,3,4
),
agg_tab as (
select
	date (visit_date) as visit_date,
	utm_source,
	utm_medium, 
	utm_campaign,
	count(visitor_id) as visitors_count,
	count (lead_id) as leads_count,
	COUNT(lead_id) FILTER (
            WHERE status_id = 142
        ) as purchases_count,
	SUM(amount) AS revenue
from ranked_clicks
where rn = 1
group by date (visit_date), utm_source, utm_medium, utm_campaign
order by revenue DESC nulls last, date (visit_date), visitors_count desc, utm_source, utm_medium, utm_campaign),
tab as (
select
	visit_date,
	agg_tab.utm_source,
	agg_tab.utm_medium,
	agg_tab.utm_campaign,
	visitors_count,
	total_cost,
	leads_count,
	purchases_count,
	revenue
from agg_tab
join spendings on agg_tab.utm_source = spendings.utm_source
            AND agg_tab.utm_medium = spendings.utm_medium
            AND agg_tab.utm_campaign = spendings.utm_campaign
            AND agg_tab.visit_date = spendings.campaign_date
order by revenue DESC nulls last, date (visit_date), visitors_count desc, utm_source, utm_medium, utm_campaign)

SELECT
    utm_source,
    CASE
        WHEN SUM(visitors_count) = 0 THEN 0
        ELSE ROUND(SUM(total_cost) / SUM(visitors_count), 2)
    END AS cpu,
    CASE
        WHEN SUM(leads_count) = 0 THEN 0
        ELSE ROUND(SUM(total_cost) / SUM(leads_count), 2)
    END AS cpl,
    CASE
        WHEN SUM(purchases_count) = 0 THEN 0
        ELSE ROUND(SUM(total_cost) / SUM(purchases_count), 2)
    END AS cppu,
    ROUND(
        100.0 * (SUM(revenue) - SUM(total_cost)) / SUM(total_cost), 2
    ) AS roi
FROM tab
GROUP BY 1
;


-- Затраты
select 
	campaign_date::date, 
	utm_source, 
	utm_medium,
	utm_campaign,
	sum (daily_spent) as total_cost
from vk_ads 
group by 1,2,3,4
order by 1
union 
	select 
		date(campaign_date) as campaign_date, 
		utm_source, 
		utm_medium,
		utm_campaign,
		sum(daily_spent) as total_cost
	from ya_ads 
	group by 1,2,3,4
order by 1;