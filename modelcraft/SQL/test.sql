WITH avg_monthly_dist_per_dollar AS
  (SELECT to_char(request_date::date, 'YYYY-MM') AS request_mnth,
          sum(distance_to_travel)/sum(monetary_cost) monthly_dist_per_dollar
   FROM uber_request_logs
   GROUP BY request_mnth
   ORDER BY request_mnth),
     naive_forecast AS
  (SELECT request_mnth,
          monthly_dist_per_dollar,
          lag(monthly_dist_per_dollar, 1) OVER (
                                             ORDER BY request_mnth) previous_mnth_dist_per_dollar
   FROM avg_monthly_dist_per_dollar),
     power AS
  (SELECT request_mnth,
          monthly_dist_per_dollar,
          previous_mnth_dist_per_dollar,
          POWER(previous_mnth_dist_per_dollar - monthly_dist_per_dollar, 2) AS power
   FROM naive_forecast
   GROUP BY request_mnth,
            monthly_dist_per_dollar,
            previous_mnth_dist_per_dollar
   ORDER BY request_mnth)
SELECT round(sqrt(avg(power))::DECIMAL ,2) as rmse
FROM power;


with agg_d as (
select
to_char(request_date,'YYYY-MM') as year_month,
sum(distance_to_travel) as agg_distance_to_travel,
sum(monetary_cost) as agg_monetary_cost,
sum(distance_to_travel) / sum(monetary_cost) as d_per_dollar
from uber_request_logs group by 1 order by 1
-- window w as (order by to_char(request_date,'YYYY-MM'))
),
forcast as (
select year_month, d_per_dollar, lag(d_per_dollar,1) over w as forcast_d_per_dollar, d_per_dollar - lag(d_per_dollar,1,0) over w as diff  from agg_d
window w as (order by year_month )
order by 1
)
select sqrt(avg(diff * diff)) from forcast;
;
