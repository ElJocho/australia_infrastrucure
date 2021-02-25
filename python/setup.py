from setuptools import setup, find_packages


console_scripts = """
    [console_scripts]
    infraustralia=infraustralia.infraustralia:cli
    """

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="infraustralia",
    version="1.0",
    description="Install script for the infraustralia package.",
    author="J. Stier, D. Wagner",
    author_email="",
    url="",
    packages=find_packages(exclude=("tests", "docs")),
    install_requires=requirements,
    entry_points=console_scripts,
)
