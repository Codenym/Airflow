select base.form_id_number,
       case when if(initial_report_indicator = '', Null, initial_report_indicator)::int =0 then False
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
       ein_uuid,
       e_mail_address,
       if(established_date = '', NULL, strptime(established_date, '%Y%m%d')::date)    as established_date,
       upper(custodian_name),
       cus_add.address_uuid                                                                as custodian_address_id,
       upper(contact_person_name),
       con_add.address_uuid                                                                as contact_address_id,
       case when if(exempt_8872_indicator='', Null, exempt_8872_indicator)::int = 0 then False
            when if(exempt_8872_indicator='', Null, exempt_8872_indicator)::int = 1 then True
                else 'Casting Error'
end
as exempt_8872_indicator,
       upper(exempt_state),
       case when if(exempt_990_indicator='', Null, exempt_990_indicator)::int = 0 then False
            when if(exempt_990_indicator='', Null, exempt_990_indicator)::int = 1 then True
                else 'Casting Error'
end
as exempt_990_indicator,
       upper(purpose),
       if(material_change_date = '', NULL, strptime(material_change_date, '%Y%m%d')::date) as material_change_date,
       insert_datetime,
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
       mail_add.address_uuid                                                               as mailing_address_id,
       bus_add.address_uuid                                                                as business_address_i
from $landing_form8871 as base
         left join $curated_eins as ein on ein.ein = base.ein and ein.organization_name = base.organization_name
         left join $curated_addresses as mail_add on ((mail_add.address_1 = mailing_address_1) or
                                                      (mail_add.address_1 is null and mailing_address_1 is null)) and
                                                     ((mail_add.address_2 = mailing_address_2) or
                                                      (mail_add.address_2 is null and mailing_address_2 is null)) and
                                                     ((mail_add.city = mailing_address_city) or
                                                      (mail_add.city is null and mailing_address_city is null)) and
                                                     ((mail_add.state = mailing_address_state) or
                                                      (mail_add.state is null and mailing_address_state is null)) and
                                                     ((mail_add.zip_code = mailing_address_zip_code) or
                                                      (mail_add.zip_code is null and mailing_address_zip_code is null)) and
                                                     ((mail_add.zip_ext = mailing_address_zip_ext) or
                                                      (mail_add.zip_ext is null and mailing_address_zip_ext is null))
         left join $curated_addresses as con_add on ((con_add.address_1 = contact_address_1) or
                                                     (con_add.address_1 is null and contact_address_1 is null)) and
                                                    ((con_add.address_2 = contact_address_2) or
                                                     (con_add.address_2 is null and contact_address_2 is null)) and
                                                    ((con_add.city = contact_address_city) or
                                                     (con_add.city is null and contact_address_city is null)) and
                                                    ((con_add.state = contact_address_state) or
                                                     (con_add.state is null and contact_address_state is null)) and
                                                    ((con_add.zip_code = contact_address_zip_code) or
                                                     (con_add.zip_code is null and contact_address_zip_code is null)) and
                                                    ((con_add.zip_ext = contact_address_zip_ext) or
                                                     (con_add.zip_ext is null and contact_address_zip_ext is null))
         left join $curated_addresses as bus_add on ((bus_add.address_1 = business_address_1) or
                                                     (bus_add.address_1 is null and business_address_1 is null)) and
                                                    ((bus_add.address_2 = business_address_2) or
                                                     (bus_add.address_2 is null and business_address_2 is null)) and
                                                    ((bus_add.city = business_address_city) or
                                                     (bus_add.city is null and business_address_city is null)) and
                                                    ((bus_add.state = business_address_state) or
                                                     (bus_add.state is null and business_address_state is null)) and
                                                    ((bus_add.zip_code = business_address_zip_code) or
                                                     (bus_add.zip_code is null and business_address_zip_code is null)) and
                                                    ((bus_add.zip_ext = business_address_zip_ext) or
                                                     (bus_add.zip_ext is null and business_address_zip_ext is null))
         left join $curated_addresses as cus_add on ((cus_add.address_1 = custodian_address_1) or
                                                     (cus_add.address_1 is null and custodian_address_1 is null)) and
                                                    ((cus_add.address_2 = custodian_address_2) or
                                                     (cus_add.address_2 is null and custodian_address_2 is null)) and
                                                    ((cus_add.city = custodian_address_city) or
                                                     (cus_add.city is null and custodian_address_city is null)) and
                                                    ((cus_add.state = custodian_address_state) or
                                                     (cus_add.state is null and custodian_address_state is null)) and
                                                    ((cus_add.zip_code = custodian_address_zip_code) or
                                                     (cus_add.zip_code is null and custodian_address_zip_code is null)) and
                                                    ((cus_add.zip_ext = custodian_address_zip_ext) or
                                                     (cus_add.zip_ext is null and custodian_address_zip_ext is null))
