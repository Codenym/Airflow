select
form_id_number,
sched_a_id,
organization_name,
ein,
contributor_name,
a.id as contributor_address_id,
contributor_employer,
contribution_amount,
contributor_occupation,
agg_contribution_ytd,
contribution_date
from $staging_form8872_schedule_a_1cast
         left join $curated_addresses a
                   on (a.state = contributor_address_state or
                       (a.state is null and contributor_address_state is null)) and
                      (a.zip_code = contributor_address_zip_code or
                       (a.zip_code is null and contributor_address_zip_code is null)) and
                      (a.city = contributor_address_city or
                       (a.city is null and contributor_address_city is null)) and
                      (a.address_1 = contributor_address_1 or
                       (a.address_1 is null and contributor_address_1 is null)) and
                      (a.address_2 = contributor_address_2 or
                       (a.address_2 is null and contributor_address_2 is null)) and
                      (a.zip_ext = contributor_address_zip_ext or
                       (a.zip_ext is null and contributor_address_zip_ext is null))


