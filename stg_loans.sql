-- stg_loans.sql
-- Staging model: cleans and standardizes raw loan application data
-- Source: raw_lending.loan_applications

with source as (
    select * from {{ source('raw_lending', 'loan_applications') }}
),

cleaned as (
    select
        -- identifiers
        loan_id::varchar                                    as loan_id,
        borrower_id::varchar                               as borrower_id,

        -- loan details
        loan_amount::numeric(12,2)                         as loan_amount,
        nullif(trim(loan_purpose), '')                     as loan_purpose,
        nullif(trim(loan_term_months), '')::int            as loan_term_months,
        nullif(trim(interest_rate), '')::numeric(5,2)      as interest_rate,

        -- borrower profile
        nullif(trim(employment_status), '')                as employment_status,
        nullif(annual_income, '')::numeric(12,2)           as annual_income,
        nullif(credit_score, '')::int                      as credit_score,
        nullif(debt_to_income_ratio, '')::numeric(5,4)     as debt_to_income_ratio,
        nullif(trim(home_ownership), '')                   as home_ownership,

        -- application outcome
        nullif(trim(loan_status), '')                      as loan_status,
        nullif(trim(grade), '')                            as risk_grade,

        -- timestamps
        application_date::date                             as application_date,
        funded_date::date                                  as funded_date,
        last_payment_date::date                            as last_payment_date

    from source
    where loan_id is not null
      and borrower_id is not null
      and loan_amount > 0
)

select * from cleaned
