-- mrt_product_metrics.sql
-- Mart: product-level KPIs for faster, more reliable decision-making
-- Used by: Product team to analyze feature launches, conversion, and funnel performance

with loan_facts as (
    select * from {{ ref('fct_loan_performance') }}
),

monthly_metrics as (
    select
        date_trunc('month', application_date)::date         as month,
        loan_purpose,
        risk_grade,
        credit_segment,
        state,

        -- volume metrics
        count(loan_id)                                      as applications,
        count(case when loan_status != 'rejected' then 1 end)  as funded_loans,
        count(case when loan_status = 'fully_paid' then 1 end) as paid_off_loans,

        -- financial metrics
        sum(loan_amount)                                    as total_loan_volume,
        avg(loan_amount)                                    as avg_loan_amount,
        avg(interest_rate)                                  as avg_interest_rate,
        avg(credit_score)                                   as avg_credit_score,
        avg(debt_to_income_ratio)                           as avg_dti,

        -- repayment health
        avg(repayment_rate_pct)                             as avg_repayment_rate,
        sum(case when loan_health_status = 'delinquent' then 1 else 0 end) as delinquent_count,
        sum(case when is_high_risk then 1 else 0 end)      as high_risk_count,

        -- conversion rate (applications → funded)
        round(
            count(case when loan_status != 'rejected' then 1 end)::numeric
            / nullif(count(loan_id), 0) * 100, 2
        )                                                   as funding_conversion_rate

    from loan_facts
    group by 1, 2, 3, 4, 5
)

select * from monthly_metrics
order by month desc, total_loan_volume desc
