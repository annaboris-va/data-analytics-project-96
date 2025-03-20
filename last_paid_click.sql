with ranked_clicks as (
select 
	s.visitor_id,
	visit_date,
	source,
	medium, 
	campaign,
	lead_id,
	created_at,
	amount,
	closing_reason,
	status_id,
	ROW_NUMBER() OVER (PARTITION BY s.visitor_id 
            ORDER BY visit_date DESC
        ) AS rn
from sessions s
left join leads l ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
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
--limit 10
;