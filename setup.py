from setuptools import find_packages, setup

setup(
    name="assets",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "boto3",
        "credstash",
        "duckdb",
        "dagster",
        "dagster_aws",
        "dagster-webserver",
        "openpyxl",
        "pandas",
        "psycopg2-binary",
        "s3fs",
        "GitPython",
        "lxml",
        "scrapelib",
        "iso8601",
        "sqlescapy",
        "huggingface_hub",
        "ezduckdb",
        "dagster_cloud",
    ],
    extras_require={"dev": ["pytest"]},
)
