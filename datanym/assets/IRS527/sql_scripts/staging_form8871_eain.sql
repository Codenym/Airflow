select trim(form_id_number)                          as form_id_number,
       trim(eain_id)                                 as eain_id,
       trim(election_authority_id_number)            as election_authority_id_number,
       upper(replace(trim(state_issued), '  ', ' ')) as state_issued
from $landing_form8871_eain

