drop table if exists form8871_directors;
create table form8871_directors
    (
        record_type       varchar(1),
        form_id_number    varchar(9),
        director_id       varchar(6) primary key,
        org_name          varchar(100),
        ein               varchar(9),
        entity_name       varchar(100),
        entity_title      varchar(100),
        entity_address_id int

--         foreign key (form_id_number) references form8871_landing (form_id_number)
--         foreign key (entity_address_id) references addresses (address_id)

    );

insert into
    form8871_directors
(record_type,
 form_id_number,
 director_id,
 org_name,
 ein,
 entity_name,
 entity_title,
 entity_address_id)

select
    record_type,
    form_id_number,
    director_id,
    org_name,
    ein,
    entity_name,
    entity_title,
    ent_add.address_id as entity_address_id
from
    landing.form8871_directors_landing
        left join addresses as ent_add on
            ((ent_add.address_1 = entity_address_1) or
             (ent_add.address_1 is null and entity_address_1 is null)) and
            ((ent_add.address_2 = entity_address_2) or
             (ent_add.address_2 is null and entity_address_2 is null)) and
            ((ent_add.city = entity_address_city) or (ent_add.city is null and entity_address_city is null)) and
            ((ent_add.state = entity_address_st) or
             (ent_add.state is null and entity_address_st is null)) and
            ((ent_add.zip_code = entity_address_zip_code) or
             (ent_add.zip_code is null and entity_address_zip_code is null)) and
            ((ent_add.zip_ext = entity_address_zip_code_ext) or
             (ent_add.zip_ext is null and entity_address_zip_code_ext is null));


select
    max(length(record_type)),
    max(length(form_id_number)),
    max(length(director_id)),
    max(length(org_name)),
    max(length(ein)),
    max(length(entity_name)),
    max(length(entity_title))
from
    landing.form8871_directors_landing
        left join addresses as ent_add on
            ((ent_add.address_1 = entity_address_1) or
             (ent_add.address_1 is null and entity_address_1 is null)) and
            ((ent_add.address_2 = entity_address_2) or
             (ent_add.address_2 is null and entity_address_2 is null)) and
            ((ent_add.city = entity_address_city) or (ent_add.city is null and entity_address_city is null)) and
            ((ent_add.state = entity_address_st) or
             (ent_add.state is null and entity_address_st is null)) and
            ((ent_add.zip_code = entity_address_zip_code) or
             (ent_add.zip_code is null and entity_address_zip_code is null)) and
            ((ent_add.zip_ext = entity_address_zip_code_ext) or
             (ent_add.zip_ext is null and entity_address_zip_code_ext is null));

