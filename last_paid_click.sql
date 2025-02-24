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
	created_at,
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
left join ya_ads ya using (utm_source,utm_medium,utm_campaign)
)
select
	visitor_id,
	visit_date,
	utm_source,
	utm_medium, 
	utm_campaign,
	lead_id,
	created_at,
	amount,
	closing_reason,
	status_id
from ranked_clicks
where rn = 1 and utm_source is not null
order by amount DESC nulls last, visit_date, utm_source, utm_medium, utm_campaign
limit 10
;
