from setuptools import setup, find_packages

setup(
    name="citibike",
    version="0.0.1",
    description="A package for processing Citibike data",
    author="R.Jha",
    packages=find_packages(where="./src"),
    package_dir={"": "./src"},
    install_requires=["setuptools"],
    entry_points={"packages": ["main = dab_project.main:main"]},
)
