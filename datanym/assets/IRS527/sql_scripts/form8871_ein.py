drop_ein = 'drop table if exists {form8871_ein};'
ddl_ein = '''
create table {form8871_ein}
    (
        eain_id        text primary key,
        form_id_number text,
        election_authority_id_number text,
        state_issued text
    );
'''

data_ein = '''
insert into {form8871_ein} (eain_id, form_id_number, election_authority_id_number, state_issued)
select
    eain_id,
    form_id_number,
    election_authority_id_number,
    state_issued
from {form8871_ein_landing}
;
'''

dagster_run_queries = [drop_ein, ddl_ein, data_ein]