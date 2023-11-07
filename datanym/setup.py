from setuptools import find_packages, setup

setup(
    name="datanym",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster",
        "pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
