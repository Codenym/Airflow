drop table if exists form8872_contributions;
CREATE TABLE form8872_contributions
    (
        contribution_id      varchar(7) primary key,
        form_id_number       varchar(7),
        contributor_id       integer,
        contribution_amount  numeric,
        agg_contribution_ytd numeric,
        contribution_date    date
--         foreign key (form_id_number) references form8872 (form_id_number),
--         foreign key (contributor_id) references form8872_contributions (contributor_id)
    );

insert into
    form8872_contributions (contribution_id, form_id_number, contributor_id, contribution_amount, agg_contribution_ytd,
                            contribution_date)
select
    sched_a_id                            as contribution_id,
    form_id_number,
    contributor_id,
    cast(contribution_amount as numeric)  as contribution_amount,
    cast(agg_contribution_ytd as numeric) as agg_contribution_ytd,
    to_date(contribution_date,'YYYYMMDD')       as contribution_date
from
    landing.form8872_schedule_a_landing
        left join form8872_contributors r
                  on (name = upper(contributor_name) or (name is null and contributor_name is null)) and
                     (address_1 = upper(contributor_address_1) or
                      (address_1 is null and contributor_address_1 is null)) and
                     (address_2 = upper(contributor_address_2) or
                      (address_2 is null and contributor_address_2 is null)) and
                     (address_city = upper(contributor_address_city) or
                      (address_city is null and contributor_address_city is null)) and
                     (address_state = upper(contributor_address_state) or
                      (address_state is null and contributor_address_state is null)) and
                     (address_zip_code = upper(contributor_address_zip_code) or
                      (address_zip_code is null and contributor_address_zip_code is null)) and
                     (address_zip_ext = upper(contributor_address_zip_ext) or
                      (address_zip_ext is null and contributor_address_zip_ext is null)) and
                     (employer = upper(contributor_employer) or
                      (employer is null and contributor_employer is null)) and
                     (occupation = upper(contributor_occupation) or
                      (occupation is null and contributor_occupation is null))
;

