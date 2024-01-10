select trim(form_id_number)                                                          as form_id_number,
       if(period_begin_date = '', NULL, strptime(period_begin_date, '%Y%m%d')::date) as period_begin_date,
       if(period_end_date = '', NULL, strptime(period_end_date, '%Y%m%d')::date)     as period_end_date,
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
        case when if(change_of_address_indicator='',Null,change_of_address_indicator)::int = 0 then False
             when if(change_of_address_indicator='',Null,change_of_address_indicator)::int = 1 then True
                else 'Casting Error'
end
as change_of_address_indicator,
upper(trim(organization_name)) as organization_name,
trim(ein) as ein,
upper(trim(mailing_address_1)) as mailing_address_1,
upper(trim(mailing_address_2)) as mailing_address_2,
upper(trim(mailing_address_city)) as mailing_address_city,
upper(trim(mailing_address_state)) as mailing_address_state,
trim(mailing_address_zip_code) as mailing_address_zip_code,
trim(mailing_address_zip_ext) as mailing_address_zip_ext,
trim(e_mail_address) as e_mail_address,

       if(org_formation_date = '', NULL, strptime(org_formation_date, '%Y%m%d')::date)         as org_formation_date,
       upper(trim(custodian_name))                                                                  as custodian_name,
upper(trim(custodian_address_1))                                                             as custodian_address_1,
upper(trim(custodian_address_2))                                                             as custodian_address_2,
upper(trim(custodian_address_city))                                                          as custodian_address_city,
upper(trim(custodian_address_state))                                                         as custodian_address_state,
trim(custodian_address_zip_code)                                                                    as custodian_address_zip_code,
trim(custodian_address_zip_ext )                                                                    as custodian_address_zip_ext,

       upper(trim(contact_person_name))                                                              as contact_person_name,
upper(trim(contact_address_1))                                                               as contact_address_1,
upper(trim(contact_address_2))                                                               as contact_address_2,
upper(trim(contact_address_city))                                                            as contact_address_city,
upper(trim(contact_address_state))                                                           as contact_address_state,
trim(contact_address_zip_code)                                                                      as contact_address_zip_code,
trim(contact_address_zip_ext)                                                                      as contact_address_zip_ext,

         upper(trim(business_address_1))                                                              as business_address_1,
upper(trim(business_address_2))                                                              as business_address_2,
upper(trim(business_address_city))                                                           as business_address_city,
upper(trim(business_address_state))                                                          as business_address_state,
trim(business_address_zip_code)                                                                     as business_address_zip_code,
trim(business_address_zip_ext)                                                                     as business_address_zip_ext,

       if(qtr_indicator = '', null, qtr_indicator::int)                                   as qtr_indicator,
       if(monthly_rpt_month = '', null, monthly_rpt_month::int)                           as monthly_rpt_month,
       upper(trim(pre_elect_type)) as pre_elect_type,
       if(pre_or_post_elect_date = '', NULL,
          strptime(pre_or_post_elect_date, '%Y%m%d')::date)                                    as pre_or_post_elect_date,
       upper(trim(pre_or_post_elect_state))                                                         as pre_or_post_elect_state,

       case when if(sched_a_ind = '', Null, sched_a_ind)::int = 0 then False
            when if(sched_a_ind = '', Null, sched_a_ind)::int = 1 then True
                else 'Casting Error'
end
as schedule_a_ind,
    total_sched_a::integer as total_sched_a,
    case when if(sched_b_ind = '', Null, sched_b_ind)::int = 0 then False
            when if(sched_b_ind = '', Null, sched_b_ind)::int = 1 then True
                else 'Casting Error'
end as schedule_b_ind,
    total_sched_b::integer as total_sched_b,
       if(insert_datetime = '', NULL, strptime(insert_datetime, '%Y-%m-%d %H:%M:%S')::timestamp) as insert_datetime


from $landing_form8872
    
