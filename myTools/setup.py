import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="myTools-yassafi",
    version="0.0.1",
    author="Yassine ASSAFI",
    author_email="yassine.assafi@edu.dsti.institute",
    description="myTools offers utility classes and functions for dealing with the Always Fresh Survey Data Project",
    url="",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
)

