select
    Session::int as session,
    strptime(Start,'%B %-d, %Y') as start_date,
    strptime("End",'%B %-d, %Y') as end_date
from $landing_house_sessions