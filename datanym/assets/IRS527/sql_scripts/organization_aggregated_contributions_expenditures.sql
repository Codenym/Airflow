drop table if exists organization_aggregated_contributions_expenditures;

create table organization_aggregated_contributions_expenditures as
    (with
         total_contributions as (select
                                     org_name,
                                     sum(contribution_amount) as total_contributions
                                 from
                                     form8872_schedule_a_landing
                                 group by org_name),
         total_expenditures as (select
                                    org_name,
                                    sum(expenditure_amount) as total_expenditures
                                from
                                    form8872_schedule_b_landing
                                group by org_name)
     select
         org_name,
         total_contributions,
         total_expenditures
     from
         total_contributions
             full outer join total_expenditures USING (org_name)
     ORDER BY
         total_contributions DESC)