with ranked_clicks as (
select 
	visitor_id,
	visit_date,
	utm_source,
	utm_medium,
	utm_campaign,
	case 
		when visit_date <= created_at then visitor_id
	else NULL
	end as lead_id,
	daily_spent,
	case
		when (closing_reason = 'Успешно реализовано' or status_id = 142) and lead_id is not null then 1
	else 0
	end as purchases,
	amount,
	closing_reason,
	status_id,
	ROW_NUMBER() OVER (PARTITION BY visitor_id 
            ORDER BY 
                CASE
                    WHEN  utm_source in ('cpc','cpm','cpa','youtube','cpp','tg','social') OR utm_medium in ('cpc','cpm','cpa','youtube','cpp','tg','social') THEN 1
                    when utm_source is not null then 0
                ELSE 0
                END DESC, 
                created_at DESC
        ) AS rn
from sessions s
left join leads l using (visitor_id)
left join vk_ads va on s.source=va.utm_source 
left join ya_ads ya using (utm_source,utm_medium,utm_campaign,daily_spent)
),
spendings as (
	select 
		campaign_date, 
		utm_source, 
		utm_medium,
		sum (daily_spent) as total_cost
	from vk_ads 
	full join ya_ads ya using (campaign_date, utm_source,utm_medium,daily_spent)
	group by campaign_date, utm_source, utm_medium
)
select
	date (visit_date) as visit_date,
	utm_source,
	utm_medium, 
	utm_campaign,
	count(visitor_id) as visitors_count,
	sum (total_cost),
	count (lead_id) as leads_count,
	sum (ranked_clicks.purchases) as purchases_count,
	sum(case 
			when ranked_clicks.purchases = 1 then amount
	else 0
	end
	) as revenue
from ranked_clicks
left join spendings using (utm_source, utm_medium)
where rn = 1 and utm_source is not null
group by date (visit_date), utm_source, utm_medium, utm_campaign
order by revenue DESC nulls last, date (visit_date), visitors_count desc, utm_source, utm_medium, utm_campaign
limit 15
;