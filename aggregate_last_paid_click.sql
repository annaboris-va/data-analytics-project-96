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
order by revenue DESC nulls last, date (visit_date), visitors_count desc, utm_source, utm_medium, utm_campaign)

select
	visit_date,
	visitors_count,
	agg_tab.utm_source,
	agg_tab.utm_medium,
	agg_tab.utm_campaign,
	total_cost,
	leads_count,
	purchases_count,
	revenue
from agg_tab
left join spendings on agg_tab.utm_source = spendings.utm_source
            AND agg_tab.utm_medium = spendings.utm_medium
            AND agg_tab.utm_campaign = spendings.utm_campaign
            AND agg_tab.visit_date = spendings.campaign_date
order by revenue DESC nulls last, date (visit_date), visitors_count desc, utm_source, utm_medium, utm_campaign
--limit 15
;