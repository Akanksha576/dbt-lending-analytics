-- mrt_risk_summary.sql
-- Mart: risk analytics for Risk team — supports underwriting and portfolio monitoring
-- Enables Risk team to identify high-risk segments and make confident lending decisions

with loan_facts as (
    select * from {{ ref('fct_loan_performance') }}
),

risk_cohorts as (
    select
        credit_segment,
        risk_grade,
        employment_status,
        loan_purpose,

        -- portfolio composition
        count(loan_id)                                              as loan_count,
        sum(loan_amount)                                            as total_exposure,
        avg(loan_amount)                                            as avg_loan_size,

        -- credit quality
        avg(credit_score)                                           as avg_credit_score,
        avg(debt_to_income_ratio)                                   as avg_dti,
        avg(annual_income)                                          as avg_annual_income,

        -- repayment performance
        avg(repayment_rate_pct)                                     as avg_repayment_rate,
        sum(outstanding_principal)                                  as total_outstanding,
        sum(case when is_high_risk then loan_amount else 0 end)     as high_risk_exposure,

        -- delinquency rates
        round(
            sum(case when loan_health_status = 'delinquent' then 1 else 0 end)::numeric
            / nullif(count(loan_id), 0) * 100, 2
        )                                                           as delinquency_rate,

        round(
            sum(case when is_high_risk then 1 else 0 end)::numeric
            / nullif(count(loan_id), 0) * 100, 2
        )                                                           as high_risk_rate,

        -- risk-adjusted return estimate
        avg(interest_rate) - (
            sum(case when loan_health_status in ('delinquent', 'charged_off_risk')
                then 1 else 0 end)::numeric / nullif(count(loan_id), 0) * 100
        )                                                           as risk_adjusted_yield

    from loan_facts
    group by 1, 2, 3, 4
)

select * from risk_cohorts
order by delinquency_rate desc
