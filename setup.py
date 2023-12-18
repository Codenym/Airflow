from setuptools import find_packages, setup

setup(
    name="assets",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "boto3",
        "credstash",
        "dagster",
        "dagster_aws",
        "openpyxl",
        "pandas",
        "psycopg2-binary",
        "s3fs",
        "GitPython",
        "lxml",
        "scrapelib",
        "iso8601",

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
