import duckdb
import boto3
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


def get_schema_table_name(key):
    file_name = key.split('/')[-1].split('.')[0]
    schema_name = file_name.split('_')[0]
    table_name = file_name.replace(schema_name + '_', '')
    return schema_name, table_name


def get_s3_path(key, s3_bucket):
    return f"s3://{s3_bucket}/{key}"


def create_object_query(key, s3_bucket, table=False):
    schema_name, table_name = get_schema_table_name(key)
    file_path = get_s3_path(key, s3_bucket)
    return f"create or replace {'table' if table else 'view'} {schema_name}.{table_name} as from read_parquet('{file_path}')"


def run_qry(qry, connection):
    print(qry)
    connection.query(qry)


if __name__ == '__main__':
    schemas = ['landing', 'dev', 'staging', 'curated', 'analytics']
    s3_bucket = 'datanym-pipeline'
    s3_prefix = 'duckdb/dev/'
    aws_profile = 'codenym'

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--filename", default="database", help="Name of the database file (without extension)")
    parser.add_argument("--create-tables", action="store_true", help="Create duckdb as tables instead of views")
    parser.add_argument("--clear-schemas", action="store_false",help="Delete landing, dev, staging, curated, analytics schemas if they exist")

    args = vars(parser.parse_args())

    with duckdb.connect(f"{args['filename']}.duckdb") as con:
        con.query("install httpfs; load httpfs;")
        con.query("install aws; load aws;")
        con.query(f"CALL load_aws_credentials('{aws_profile}');")
        s3 = boto3.Session(profile_name=aws_profile).resource('s3')

        existing_schemas = con.query('select distinct schema_name from information_schema.schemata').fetchall()
        existing_schemas = [o[0] for o in existing_schemas]
        for schema in schemas:
            if args["clear_schemas"] and schema in existing_schemas:
                run_qry(f"DROP SCHEMA {schema} CASCADE;", con)
            if args['clear_schemas'] or (schema not in existing_schemas):
                run_qry(f"CREATE SCHEMA {schema};", con)

        bucket = s3.Bucket(s3_bucket)
        for obj in bucket.objects.filter(Prefix=s3_prefix):
            run_qry(create_object_query(obj.key, s3_bucket, args["create_tables"]), con)

        print(f"Done.  Created DuckDB database at {args['filename']}")




