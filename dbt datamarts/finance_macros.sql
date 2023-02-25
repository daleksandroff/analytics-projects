/* Model for Weekly, Monthly reports
*/

{% macro looker_fin_ops(date_granularity, periods_field_name) %}

WITH main as (

  SELECT date_trunc('{{ date_granularity }}', d.day)::date AS {{ periods_field_name }}
      , d.segment
      , d.country AS country
      , d.city AS city
      , SUM(d.n_journeys) AS n_journeys

  {% for metric in ('gross_bookings', 'parking', 'toll_roads', 'tips', 'net_bookings'
                    , 'gross_revenue', 'chauffeur_fines'
                    , 'net_revenue_b2c', 'net_revenue_b2b', 'net_revenue_total', 'passenger_bonuses', 'vat', 'chauffeur_total_bonuses') %}
    {% for currency in ('rub', 'gbp', 'eur', 'usd') %}
      , SUM(d.{{metric}}_{{currency}}) AS {{metric}}_{{currency}}
  {% endfor %}
    {% endfor %}

  FROM {{ref('f_finance_metrics_daily')}} d

  GROUP BY 1, 2, 3, 4
  ORDER BY 1
)

, partnership_data AS (

  SELECT date_trunc('{{ date_granularity }}', (p.completed_ts_loc)::timestamp)::date AS {{ periods_field_name }}
        , j.segment
        , j.country
        , j.city

      {% for currency in ('rub', 'gbp', 'eur', 'usd') %}
        , SUM(CASE WHEN p.partner_name = 'PARTNER1' THEN p.partner_cost_{{currency}} END) AS PARTNER1_privileges_revenue_{{currency}}
        , SUM(CASE WHEN p.partner_name = 'PARTNER2' THEN p.partner_cost_{{currency}} END) AS PARTNER2_privileges_revenue_{{currency}}
        , SUM(CASE WHEN p.partner_name = 'PARTNER3' THEN p.partner_cost_{{currency}} END) AS PARTNER3_privileges_revenue_{{currency}}
        
        , SUM(CASE WHEN p.partner_name NOT IN ('PARTNER1', 'PARTNER2', 'PARTNER3')
                   THEN p.partner_cost_{{currency}} END) AS other_privileges_revenue_{{currency}}

        , SUM(p.partner_cost_{{currency}}) AS privileges_revenue_total_{{currency}}
      {% endfor %}

  FROM {{ref('f_partnerships_journeys_costs')}} p

  INNER JOIN {{ref('f_passengers_journeys')}} j
    ON p.journey_id = j.journey_id

  GROUP BY 1, 2, 3, 4
)

  SELECT m.{{periods_field_name}}
      , m.segment
      , m.country
      , m.city
      , m.n_journeys

    {% for currency in ('rub', 'gbp', 'eur', 'usd') %}
      , m.gross_bookings_{{currency}}
      , m.parking_{{currency}}
      , m.toll_roads_{{currency}}
      , m.tips_{{currency}}
      , m.net_bookings_{{currency}}
      , m.gross_revenue_{{currency}}
      , COALESCE(m.chauffeur_total_bonuses_{{currency}}, 0) AS chauffeur_total_bonuses_{{currency}}
      , COALESCE(m.passenger_bonuses_{{currency}}, 0) passenger_bonuses_{{currency}}
      -- PRIVILEGES
      , COALESCE(pd.PARTNER1_privileges_revenue_{{currency}}, 0) PARTNER1_privileges_revenue_{{currency}}
      , COALESCE(pd.PARTNER2_privileges_revenue_{{currency}}, 0) PARTNER2_privileges_revenue_{{currency}}
      , COALESCE(pd.PARTNER3_privileges_revenue_{{currency}}, 0) PARTNER3_privileges_revenue_{{currency}}
      , COALESCE(pd.other_privileges_revenue_{{currency}}, 0) other_privileges_revenue_{{currency}}

      , COALESCE(pd.privileges_revenue_total_{{currency}}, 0) privileges_revenue_total_{{currency}}
      , m.net_revenue_b2c_{{currency}}
      , m.net_revenue_b2b_{{currency}}
      , m.net_revenue_total_{{currency}}
      , COALESCE(m.chauffeur_fines_{{currency}}, 0) chauffeur_fines_{{currency}}
      , COALESCE(chauffeur_fines_{{currency}}, 0) - COALESCE(chauffeur_total_bonuses_{{currency}}, 0) AS chauffeurs_fines_minus_bonuses_{{currency}}
    {% endfor %}

  FROM main m

  LEFT JOIN partnership_data pd
    ON m.{{periods_field_name}} = pd.{{periods_field_name}}
    AND m.segment = pd.segment
    AND m.country = pd.country
    AND m.city = pd.city


{% endmacro %}
