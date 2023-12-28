# Codenym Data Pielines

## Dataset Requests

If you have a request for a dataset to be created, please create an issue.  It is much more likely to be a priority if you include the following information:
    + A few sentances of background about it what the data is for some context
    + A few sentances or bullets about what things could be done with it if it existed and why that matters
    + Links to good information we can learn more about it
    + Thoughts on where data could be sourced from, or any thoughts on how the dataset would be created

If it's clear that it's feasible and impactful that'd go a long way to getting it higher on the priority list.

## Contributions

If you'd like to contribute, you should start an issue explaining what you'd like to do.  We can chat then about direction and next steps.

## Pipelines 
### IRS 527 Pipeline

+ **Code:** [Pipeline Code](datanym/assets/IRS527)
+ **Source:** [IRS political organizations website](https://www.irs.gov/charities-non-profits/political-organizations/political-organization-filing-and-disclosure)

#### Form 8871

Used by political organizations to notify the IRS of their status as tax-exempt under section 527.  Includes the organization's mailing address, email address, custodian of records, contact person, and any material changes to previously reported information.  It also 3 types of child records that can be joined to the parent 8871 form.  These are directors and officers, related entities, and EINs.

#### Form 8872

Required for section 527 tax-exempt political organizations to report certain received contributions and made expenditures.  Organizations must file this form alongside Form 8871 to report contributions and expenditures, especially when there are changes to the organization's status or operations.  Form 8872 has 2 types of child records, Schedule A (Itemized Contributions) and Schedule B (Itemized Expenditures).

## Database Usage

1. Connect to database.duckdb with dbeaver or other SQL client
2. Run the following query to authenticate with aws:
```sql  
CALL load_aws_credentials('codenym')
```
