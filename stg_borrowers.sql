-- stg_borrowers.sql
-- Staging model: cleans and standardizes borrower profile data
-- Source: raw_lending.borrower_profiles

with source as (
    select * from {{ source('raw_lending', 'borrower_profiles') }}
),

cleaned as (
    select
        borrower_id::varchar                               as borrower_id,
        nullif(trim(state), '')                            as state,
        nullif(trim(zip_code), '')                         as zip_code,
        nullif(years_employed, '')::numeric(4,1)           as years_employed,
        nullif(total_accounts, '')::int                    as total_accounts,
        nullif(open_accounts, '')::int                     as open_accounts,
        nullif(derogatory_marks, '')::int                  as derogatory_marks,

        -- segment borrower by credit quality
        case
            when nullif(credit_score, '')::int >= 750 then 'prime'
            when nullif(credit_score, '')::int >= 670 then 'near_prime'
            when nullif(credit_score, '')::int >= 580 then 'subprime'
            else 'deep_subprime'
        end                                                as credit_segment,

        created_at::timestamp                              as created_at

    from source
    where borrower_id is not null
)

select * from cleaned
