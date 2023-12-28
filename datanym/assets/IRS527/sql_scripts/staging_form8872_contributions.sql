select
    sched_a_id                            as contribution_id,
    form_id_number,
    contributor_id,
    cast(case when contribution_amount = '' then null else contribution_amount end as numeric)  as contribution_amount,
    cast(case when agg_contribution_ytd = '' then null else agg_contribution_ytd end as numeric) as agg_contribution_ytd,
    case when contribution_date = '' then NULL else strptime(contribution_date,'%Y%m%d')::date end       as contribution_date
from
    $landing_form8872_schedule_a
        left join $staging_form8872_contributors r
                  on (name = upper(contributor_name) or (name is null and contributor_name is null)) and
                     (address_1 = upper(contributor_address_1) or
                      (address_1 is null and contributor_address_1 is null)) and
                     (address_2 = upper(contributor_address_2) or
                      (address_2 is null and contributor_address_2 is null)) and
                     (address_city = upper(contributor_address_city) or
                      (address_city is null and contributor_address_city is null)) and
                     (address_st = upper(contributor_address_state) or
                      (address_st is null and contributor_address_state is null)) and
                     (address_zip_code = upper(contributor_address_zip_code) or
                      (address_zip_code is null and contributor_address_zip_code is null)) and
                     (address_zip_ext = upper(contributor_address_zip_ext) or
                      (address_zip_ext is null and contributor_address_zip_ext is null)) and
                     (employer = upper(contributor_employer) or
                      (employer is null and contributor_employer is null)) and
                     (occupation = upper(contributor_occupation) or
                      (occupation is null and contributor_occupation is null))


