drop table if exists form8872_recipients;
create table form8872_recipients
    (
        recipient_id     integer primary key autoincrement,
        name             text,
        address_1        text,
        address_2        text,
        address_city     text,
        address_state    text,
        address_zip_code text,
        address_zip_ext  text,
        employer         text,
        occupation       text
    );


insert into
    form8872_recipients
(name, address_1, address_2, address_city, address_state, address_zip_code, address_zip_ext,
 employer, occupation)

select distinct
    upper(reciepient_name)             as name,
    upper(reciepient_address_1)        as address_1,
    upper(reciepient_address_2)        as address_2,
    upper(reciepient_address_city)     as address_city,
    upper(reciepient_address_st)       as address_st,
    upper(reciepient_address_zip_code) as address_zip_code,
    upper(reciepient_address_zip_ext)  as address_zip_ext,
    upper(reciepient_employer)         as employer,
    upper(recipient_occupation)        as occupation
from
    form8872_schedule_b_landing;
