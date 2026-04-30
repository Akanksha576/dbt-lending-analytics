-- stg_payments.sql
-- Staging model: cleans raw payment transaction data
-- Source: raw_lending.payment_transactions

with source as (
    select * from {{ source('raw_lending', 'payment_transactions') }}
),

cleaned as (
    select
        payment_id::varchar                                as payment_id,
        loan_id::varchar                                   as loan_id,
        borrower_id::varchar                               as borrower_id,

        payment_date::date                                 as payment_date,
        nullif(payment_amount, '')::numeric(12,2)          as payment_amount,
        nullif(principal_amount, '')::numeric(12,2)        as principal_amount,
        nullif(interest_amount, '')::numeric(12,2)         as interest_amount,

        nullif(trim(payment_status), '')                   as payment_status,
        nullif(days_past_due, '')::int                     as days_past_due,

        -- derived quality flag
        case
            when nullif(days_past_due, '')::int > 30  then 'delinquent'
            when nullif(days_past_due, '')::int > 0   then 'late'
            else 'current'
        end                                                as payment_health

    from source
    where payment_id is not null
      and loan_id is not null
      and nullif(payment_amount, '')::numeric(12,2) > 0
)

select * from cleaned
