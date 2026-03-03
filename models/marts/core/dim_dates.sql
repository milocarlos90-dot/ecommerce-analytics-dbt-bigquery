with date_spine as (

    select
        cast(date_day as date) as date_day
    from (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2018-01-01' as date)",
            end_date="cast('2030-12-31' as date)"
        ) }}
    )

),

final as (

    select
        date_day,

        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,

        extract(day from date_day) as day_of_month,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%A', date_day) as day_name,

        extract(week from date_day) as week_of_year,

        case when extract(dayofweek from date_day) in (1,7)
            then true else false end as is_weekend,

        date_trunc(date_day, month) = date_day as is_month_start,
        last_day(date_day, month) = date_day as is_month_end,

        date_trunc(date_day, quarter) = date_day as is_quarter_start,
        last_day(date_day, quarter) = date_day as is_quarter_end,

        date_trunc(date_day, year) = date_day as is_year_start,
        last_day(date_day, year) = date_day as is_year_end,

        format_date('%Y-%m', date_day) as year_month,
        date_trunc(date_day, week(monday)) as week_start_date,
        date_trunc(date_day, month) as month_start_date

    from date_spine

)

select *
from final