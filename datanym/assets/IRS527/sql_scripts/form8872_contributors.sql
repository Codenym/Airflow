drop table if exists form8872_contributors;
create table form8872_contributors
    (
        contributor_id   integer primary key identity(1,1) not null,
        name             varchar(50),
        address_1        varchar(50),
        address_2        varchar(50),
        address_city     varchar(50),
        address_state    varchar(2),
        address_zip_code varchar(5),
        address_zip_ext  varchar(4),
        employer         varchar(72),
        occupation       varchar(72)
    );

insert into
    form8872_contributors
(name, address_1, address_2, address_city, address_state, address_zip_code, address_zip_ext,
 employer, occupation)
    select distinct
         upper(contributor_name)             as name,
         upper(contributor_address_1)        as address_1,
         upper(contributor_address_2)        as address_2,
         upper(contributor_address_city)     as address_city,
         upper(contributor_address_state)    as address_st,
         upper(contributor_address_zip_code) as address_zip_code,
         upper(contributor_address_zip_ext)  as address_zip_ext,
         upper(contributor_employer)         as employer,
         upper(contributor_occupation)       as occupation
     from
        landing.form8872_schedule_a_landing
;
