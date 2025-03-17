with ranked_clicks as (
select 
	visitor_id,
	visit_date,
	source,
	medium, 
	campaign,
	case 
		when visit_date <= created_at then l.lead_id
	else NULL
	end as lead_id,
	created_at,
	amount,
	closing_reason,
	status_id,
	ROW_NUMBER() OVER (PARTITION BY visitor_id 
            ORDER BY created_at DESC
        ) AS rn
from sessions s
full join leads l using (visitor_id)
where medium<>'organic'
)
select
	visitor_id,
	visit_date,
	source as utm_source,
	medium as utm_medium, 
	campaign as utm_campaign,
	lead_id,
	created_at,
	amount,
	closing_reason,
	status_id
from ranked_clicks
where rn = 1
order by amount DESC nulls last, visit_date, utm_source, utm_medium, utm_campaign
limit 10
;