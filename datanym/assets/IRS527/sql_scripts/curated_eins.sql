select uuid() as ein_uuid,
       *
from (select distinct ein,
                      upper(organization_name) as organization_name
      from $landing_form8871
      union
      select distinct ein,
                      upper(org_name) as organization_name
      from $landing_form8871_directors
      union
      select distinct ein,
                      upper(org_name) as organization_name
      from $landing_form8871_related_entities
      union
      select distinct ein,
                      upper(organization_name) as organization_name
      from $landing_form8872)