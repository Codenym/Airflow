select distinct eain_id,
                election_authority_id_number,
                upper(state_issued) as state_issued
from $landing_form8871_eain

