-- fct_loan_performance.sql
-- Fact table: core loan performance metrics used by Product, Risk, and Analytics teams
-- Powers: loan health dashboards, risk scoring, repayment analysis

with loans as (
    select * from {{ ref('stg_loans') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

borrowers as (
    select * from {{ ref('stg_borrowers') }}
),

payment_agg as (
    select
        loan_id,
        count(payment_id)                                          as total_payments_made,
        sum(payment_amount)                                        as total_amount_paid,
        sum(principal_amount)                                      as total_principal_paid,
        sum(interest_amount)                                       as total_interest_paid,
        max(payment_date)                                          as last_payment_date,
        max(days_past_due)                                         as max_days_past_due,
        sum(case when payment_health = 'delinquent' then 1 else 0 end) as delinquent_payments,
        sum(case when payment_health = 'late' then 1 else 0 end)       as late_payments
    from payments
    group by loan_id
),

final as (
    select
        -- keys
        l.loan_id,
        l.borrower_id,

        -- loan attributes
        l.loan_amount,
        l.loan_purpose,
        l.loan_term_months,
        l.interest_rate,
        l.loan_status,
        l.risk_grade,
        l.application_date,
        l.funded_date,

        -- borrower profile
        b.state,
        b.credit_segment,
        l.credit_score,
        l.annual_income,
        l.debt_to_income_ratio,
        l.employment_status,
        b.years_employed,
        b.derogatory_marks,

        -- payment performance
        coalesce(p.total_payments_made, 0)      as total_payments_made,
        coalesce(p.total_amount_paid, 0)        as total_amount_paid,
        coalesce(p.total_principal_paid, 0)     as total_principal_paid,
        coalesce(p.total_interest_paid, 0)      as total_interest_paid,
        coalesce(p.max_days_past_due, 0)        as max_days_past_due,
        coalesce(p.delinquent_payments, 0)      as delinquent_payments,
        coalesce(p.late_payments, 0)            as late_payments,

        -- derived metrics for analytics
        l.loan_amount - coalesce(p.total_principal_paid, 0)    as outstanding_principal,

        case
            when coalesce(p.max_days_past_due, 0) > 90  then 'charged_off_risk'
            when coalesce(p.max_days_past_due, 0) > 30  then 'delinquent'
            when l.loan_status = 'fully_paid'            then 'paid_off'
            else 'current'
        end                                                    as loan_health_status,

        -- repayment rate
        case
            when l.loan_amount > 0
            then round(coalesce(p.total_amount_paid, 0) / l.loan_amount * 100, 2)
            else 0
        end                                                    as repayment_rate_pct,

        -- risk flag for Risk team
        case
            when l.debt_to_income_ratio > 0.40
              or coalesce(p.max_days_past_due, 0) > 30
              or l.credit_score < 620
            then true
            else false
        end                                                    as is_high_risk

    from loans l
    left join payment_agg p on l.loan_id = p.loan_id
    left join borrowers b   on l.borrower_id = b.borrower_id
)

select * from final
