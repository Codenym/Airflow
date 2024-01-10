select
    trim(form_id_number) as form_id_number,
       case when if(initial_report_indicator = '', Null, initial_report_indicator)::int = 0 then False
            when if(initial_report_indicator = '',Null,initial_report_indicator)::int = 1 then True
                else 'Casting Error'
end
as initial_report_indicator,
         case when if(amended_report_indicator='',Null,amended_report_indicator)::int = 0 then False
              when if(amended_report_indicator='',Null,amended_report_indicator)::int = 1 then True
                else 'Casting Error'
end
as amended_report_indicator,
         case when if(final_report_indicator='',Null,final_report_indicator)::int = 0 then False
              when if(final_report_indicator='',Null,final_report_indicator)::int = 1 then True
                else 'Casting Error'
end
as final_report_indicator,
        trim(ein) as ein,
upper(trim(organization_name)) as organization_name,
upper(trim(mailing_address_1)) as mailing_address_1,
upper(trim(mailing_address_2)) as mailing_address_2,
upper(trim(mailing_address_city)) as mailing_address_city,
upper(trim(mailing_address_state)) as mailing_address_state,
trim(mailing_address_zip_code) as mailing_address_zip_code,
trim(mailing_address_zip_ext) as mailing_address_zip_ext,
       trim(e_mail_address) as e_mail_address,
       if(established_date = '', NULL, strptime(established_date, '%Y%m%d')::date)    as established_date,
       upper(trim(custodian_name)),
upper(trim(custodian_address_1)) as custodian_address_1,
upper(trim(custodian_address_2)) as custodian_address_2,
upper(trim(custodian_address_city)) as custodian_address_city,
upper(trim(custodian_address_state)) as custodian_address_state,
custodian_address_zip_code as custodian_address_zip_code,
custodian_address_zip_ext as custodian_address_zip_ext,
       upper(trim(contact_person_name)) as contact_person_name,
upper(trim(contact_address_1)) as contact_address_1,
upper(trim(contact_address_2)) as contact_address_2,
upper(trim(contact_address_city)) as contact_address_city,
upper(trim(contact_address_state)) as contact_address_state,
contact_address_zip_code as contact_address_zip_code,
contact_address_zip_ext as contact_address_zip_ext,
upper(trim(business_address_1)) as business_address_1,
upper(trim(business_address_2)) as business_address_2,
upper(trim(business_address_city)) as business_address_city,
upper(trim(business_address_state)) as business_address_state,
business_address_zip_code as business_address_zip_code,
business_address_zip_ext as business_address_zip_ext,
       case when if(exempt_8872_indicator='', Null, exempt_8872_indicator)::int = 0 then False
            when if(exempt_8872_indicator='', Null, exempt_8872_indicator)::int = 1 then True
                else 'Casting Error'
end
as exempt_8872_indicator,
       upper(trim(exempt_state)) as exempt_state,
       case when if(exempt_990_indicator='', Null, exempt_990_indicator)::int = 0 then False
            when if(exempt_990_indicator='', Null, exempt_990_indicator)::int = 1 then True
                else 'Casting Error'
end
as exempt_990_indicator,
       upper(trim(purpose)) as purpose,
       if(material_change_date = '', NULL, strptime(material_change_date, '%Y%m%d')::date) as material_change_date,
       if(insert_datetime = '', NULL, strptime(insert_datetime, '%Y-%m-%d %H:%M:%S')::timestamp) as insert_datetime,

       case when if(related_entity_bypass='', Null, related_entity_bypass)::int = 0 then False
            when if(related_entity_bypass='', Null, related_entity_bypass)::int = 1 then True
                else 'Casting Error'
end
as related_entity_bypass,
       case when if(eain_bypass='', Null, eain_bypass)::int = 0 then False
            when if(eain_bypass='', Null, eain_bypass)::int = 1 then True
                else 'Casting Error'
end
as eain_bypass,
from $landing_form8871