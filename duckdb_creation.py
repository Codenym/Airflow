import duckdb
import boto3

schemas = ['landing', 'staging', 'curated', 'analytics']
s3_bucket = 'datanym-pipeline'
s3_prefix = 'duckdb/'
aws_profile = ''

def create_view_query(key):
    file_name = key.split('/')[1].split('.')[0]
    schema_name = file_name.split('_')[0]
    assert schema_name in schemas
    table_name = file_name.replace(schema_name+'_','')
    file_path = f"s3://{obj.bucket_name}/{obj.key}"
    return f"create or replace view {schema_name}.{table_name} as (select * from read_parquet('{file_path}'))"

if __name__ == '__main__':
    with duckdb.connect("database.duckdb") as con:
        con.query("install httpfs; load httpfs;")
        con.query("install aws; load aws;")
        if aws_profile == '':
            con.query("CALL load_aws_credentials();")
            s3 = boto3.Session().resource('s3')
        else:
            con.query(f"CALL load_aws_credentials('{aws_profile}');")
            s3 = boto3.Session(profile_name=aws_profile).resource('s3')

        existing_schemas = con.query('select distinct schema_name from information_schema.schemata').fetchall()
        existing_schemas = [o[0] for o in existing_schemas]
        for schema in schemas:
            if schema in existing_schemas:
                con.query(f"DROP SCHEMA {schema} CASCADE;")
            con.query(f"CREATE SCHEMA {schema};")

        bucket = s3.Bucket(s3_bucket)
        for obj in bucket.objects.filter(Prefix=s3_prefix):
            con.query(create_view_query(obj.key))



