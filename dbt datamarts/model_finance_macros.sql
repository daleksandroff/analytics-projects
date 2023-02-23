/* Model for Weekly, Monthly reports
*/

{% macro looker_fin_ops(date_granularity, periods_field_name) %}

WITH main as (

  SELECT date_trunc('{{ date_granularity }}', d.day)::date AS {{ periods_field_name }}
      , d.segment
      , d.country AS country
      , d.city AS city
      , SUM(d.n_journeys) AS n_journeys

  {% for metric in ('gross_bookings', 'parking', 'toll_roads', 'tips', 'net_bookings', 'transfer_first_lane_price'
                    , 'gross_revenue', 'gross_revenue_b2b', 'service_charge', 'employees_gross_bookings'
                    , 'net_revenue_b2c', 'net_revenue_b2b', 'net_revenue_total', 'passenger_bonuses', 'vat', 'chauffeur_total_bonuses'
                    , 'chauffeur_performance_bonuses', 'chauffeur_nonperformance_bonuses', 'chauffeur_fines'
                    , 'errand_commission', 'errand_transaction_costs' ) %}
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
      , (m.gross_revenue_rub - COALESCE(m.chauffeur_performance_bonuses_rub, 0)) / NULLIF(m.net_bookings_rub, 0) AS take_rate

    {% for currency in ('rub', 'gbp', 'eur', 'usd') %}
      , m.gross_bookings_{{currency}}
      , m.parking_{{currency}}
      , m.toll_roads_{{currency}}
      , m.tips_{{currency}}
      , m.net_bookings_{{currency}}
      , m.gross_revenue_{{currency}}
      , COALESCE(m.service_charge_{{currency}}, 0) service_charge_{{currency}}
      , COALESCE(m.chauffeur_performance_bonuses_{{currency}}, 0) AS chauffeur_performance_bonuses_{{currency}}
      , COALESCE(m.chauffeur_nonperformance_bonuses_{{currency}}, 0) AS chauffeur_nonperformance_bonuses_{{currency}}
      , COALESCE(m.chauffeur_total_bonuses_{{currency}}, 0) AS chauffeur_total_bonuses_{{currency}}
      , COALESCE(m.passenger_bonuses_{{currency}}, 0) passenger_bonuses_{{currency}}
      -- PRIVILEGES
      , COALESCE(pd.PARTNER1_privileges_revenue_{{currency}}, 0) PARTNER1_privileges_revenue_{{currency}}
      , COALESCE(pd.PARTNER2_privileges_revenue_{{currency}}, 0) PARTNER2_privileges_revenue_{{currency}}
      , COALESCE(pd.PARTNER3_privileges_revenue_{{currency}}, 0) PARTNER3_privileges_revenue_{{currency}}
      , COALESCE(pd.other_privileges_revenue_{{currency}}, 0) other_privileges_revenue_{{currency}}

      , COALESCE(pd.privileges_revenue_total_{{currency}}, 0) privileges_revenue_total_{{currency}}
      , COALESCE(m.employees_gross_bookings_{{currency}}, 0) employees_gross_bookings_{{currency}}
      , m.net_revenue_b2c_{{currency}}
      , m.net_revenue_b2b_{{currency}}
      , m.net_revenue_total_{{currency}}
      , m.vat_{{currency}}
      , COALESCE(m.net_revenue_total_{{currency}}, 0) - COALESCE(m.vat_{{currency}}, 0) revenue_{{currency}}
      , COALESCE(m.chauffeur_fines_{{currency}}, 0) chauffeur_fines_{{currency}}
      , COALESCE(m.transfer_first_lane_price_{{currency}}, 0) transfer_first_lane_price_{{currency}}
      , COALESCE(gross_revenue_{{currency}}, 0) - COALESCE(passenger_bonuses_{{currency}}, 0) + COALESCE(service_charge_{{currency}}, 0) AS net_revenue_accounting_{{currency}}
      , COALESCE(chauffeur_fines_{{currency}}, 0) - COALESCE(chauffeur_total_bonuses_{{currency}}, 0) AS chauffeurs_fines_minus_bonuses_{{currency}}
      , COALESCE(m.gross_bookings_{{currency}}, 0) - COALESCE(gross_revenue_{{currency}}, 0) + COALESCE(chauffeur_total_bonuses_{{currency}}, 0) - COALESCE(chauffeur_fines_{{currency}}, 0) - COALESCE(transfer_first_lane_price_{{currency}}, 0) AS chauffeurs_payout_{{currency}}
      , COALESCE(m.errand_commission_{{currency}}, 0) AS errand_commission_{{currency}}
      , COALESCE(m.errand_transaction_costs_{{currency}}, 0) AS errand_transaction_costs_{{currency}}
    {% endfor %}

  FROM main m

  LEFT JOIN partnership_data pd
    ON m.{{periods_field_name}} = pd.{{periods_field_name}}
    AND m.segment = pd.segment
    AND m.country = pd.country
    AND m.city = pd.city


{% endmacro %}
