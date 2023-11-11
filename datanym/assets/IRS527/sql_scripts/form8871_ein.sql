drop table if exists form8871_ein;
create table form8871_ein
    (
        eain_id        text primary key,
        form_id_number text,
        election_authority_id_number text,
        state_issued text,
        foreign key (form_id_number) references form8871 (form_id_number)

    );

insert into form8871_ein (eain_id, form_id_number, election_authority_id_number, state_issued)
select
    eain_id,
    form_id_number,
    election_authority_id_number,
    state_issued
from form8871_ein_landing
;