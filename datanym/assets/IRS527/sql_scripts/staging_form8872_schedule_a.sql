select trim(form_id_number)                                                          as form_id_number,
       trim(sched_a_id)                                                              as sched_a_id,
       upper(trim(org_name))                                                         as organization_name,
       trim(ein)                                                                     as ein,
       upper(trim(contributor_name))                                                 as contributor_name,
       upper(trim(contributor_address_1))                                            as contributor_address_1,
       upper(trim(contributor_address_2))                                            as contributor_address_2,
       upper(trim(contributor_address_city))                                         as contributor_address_city,
       upper(trim(contributor_address_state))                                        as contributor_address_state,
       trim(contributor_address_zip_code)                                            as contributor_address_zip_code,
       trim(contributor_address_zip_ext)                                             as contributor_address_zip_ext,
       upper(trim(contributor_employer))                                             as contributor_employer,
       (contribution_amount ::numeric)                                               as contribution_amount,
       upper(trim(contributor_occupation))                                           as contributor_occupation,
       (agg_contribution_ytd ::numeric)                                              as agg_contribution_ytd,
       if(contribution_date = '', NULL, strptime(contribution_date, '%Y%m%d')::date) as contribution_date,
from $landing_form8872_schedule_a

