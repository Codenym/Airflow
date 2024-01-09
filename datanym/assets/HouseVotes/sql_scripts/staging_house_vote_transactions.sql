select 
    case when id = 'L000555' then 'L000595' else id end as voter_id,
    vote_id, 
    vote_type as raw_vote_type,
    case 
        when vote_type in ('Yea', 'Aye') then 'For' 
        when vote_type in ('Nay', 'No')  then 'Against' 
        when vote_type in ('Not Voting', 'Present') then 'Not Voting' 
        else vote_type 
    end as vote_type
from $landing_house_vote_transactions
