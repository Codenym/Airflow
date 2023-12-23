drop table if exists form8871_ein;
create table form8871_ein
    (
        eain_id        varchar(5) primary key,
        form_id_number varchar(7),
        election_authority_id_number varchar(50),
        state_issued varchar(2)
--         foreign key (form_id_number) references form8871 (form_id_number)

    );

insert into form8871_ein (eain_id, form_id_number, election_authority_id_number, state_issued)
select
    eain_id,
    form_id_number,
    election_authority_id_number,
    state_issued
from landing.form8871_ein_landing
;