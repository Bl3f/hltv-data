from setuptools import find_packages, setup

setup(
    name="hltv",
    packages=find_packages(exclude=["hltv_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-gcp",
        "pandas",
        "requests",
        "beautifulsoup4",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
