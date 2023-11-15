
drop_8871 = 'drop table if exists {form8871_landing};'
drop_ein = 'drop table if exists {form8871_ein_landing};'
drop_dir = 'drop table if exists {form8871_directors_landing};'
drop_rel = 'drop table if exists {form8871_related_entities_landing};'
drop_8872 = 'drop table if exists {form8872_landing};'
drop_a = 'drop table if exists {form8872_schedule_a_landing};'
drop_b = 'drop table if exists {form8871_schedule_b_landing};'

dagster_run_queries = [drop_8871, drop_ein, drop_dir, drop_rel, drop_8872, drop_a, drop_b]