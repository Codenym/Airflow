from setuptools import find_packages, setup

setup(
    name="assets",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster",
        "dagster_aws",
        "openpyxl",
        "pandas",

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
