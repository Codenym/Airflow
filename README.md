# Codenym Data Pielines

This is a [Dagster](https://dagster.io/) project.

## IRS 527 Pipeline

+ **Code:** [Pipeline Code](datanym/assets/IRS527)
+ **Data Source:** [IRS political action website](https://www.irs.gov/charities-non-profits/political-organizations/political-organization-filing-and-disclosure)

### Form 8871: 

Used by political organizations to notify the IRS of their status as tax-exempt under section 527.  Includes the organization's mailing address, email address, custodian of records, contact person, and any material changes to previously reported information.  It also 3 types of child records that can be joined to the parent 8871 form.  These are directors and officers, related entities, and EINs.

### Form 8872

Required for section 527 tax-exempt political organizations to report certain received contributions and made expenditures.  Organizations must file this form alongside Form 8871 to report contributions and expenditures, especially when there are changes to the organization's status or operations.  Form 8872 has 2 types of child records, Schedule A (Itemized Contributions) and Schedule B (Itemized Expenditures).
