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
WITH ranked_clicks AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.amount,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN
        leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
    WHERE s.medium != 'organic'
),

spendings AS (
    SELECT
        DATE(campaign_date) AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM vk_ads
    GROUP BY 1, 2, 3, 4
    UNION
    SELECT
        DATE(campaign_date) AS campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS total_cost
    FROM ya_ads
    GROUP BY 1, 2, 3, 4
),

agg_tab AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        DATE(visit_date) AS visit_date,
        COUNT(visitor_id) AS visitors_count,
        COUNT(lead_id) AS leads_count,
        COUNT(lead_id) FILTER (
            WHERE status_id = 142
        ) AS purchases_count,
        SUM(amount) AS revenue
    FROM ranked_clicks
    WHERE rn = 1
    GROUP BY 1, 2, 3, 4
    ORDER BY
        8 DESC NULLS LAST, 4, 5 DESC, 1 ASC, 2 ASC, 3 ASC
),

tab AS (
    SELECT
        agg_tab.visit_date,
        agg_tab.utm_source,
        agg_tab.utm_medium,
        agg_tab.utm_campaign,
        agg_tab.visitors_count,
        sp.total_cost,
        agg_tab.leads_count,
        agg_tab.purchases_count,
        agg_tab.revenue
    FROM agg_tab
    INNER JOIN spendings AS sp
        ON
            agg_tab.utm_source = sp.utm_source
            AND agg_tab.utm_medium = sp.utm_medium
            AND agg_tab.utm_campaign = sp.utm_campaign
            AND agg_tab.visit_date = sp.campaign_date
    ORDER BY 9 DESC NULLS LAST, 1, 5 DESC, 2, 3, 4
)

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
GROUP BY 1;

-- Затраты
SELECT
    campaign_date::date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
FROM vk_ads
GROUP BY 1, 2, 3, 4
UNION
SELECT
    campaign_date::date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_cost
FROM ya_ads
GROUP BY 1, 2, 3, 4
ORDER BY 1;
