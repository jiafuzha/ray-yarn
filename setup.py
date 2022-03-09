from setuptools import find_packages, setup
import re

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

with open("README.rst") as f:
    long_description = f.read()

target_file = "./ray_yarn/__init__.py"

version = "Unknown"
version_pattern = re.compile(r"^[^#]*__version__[^=]*=\s*(\S+)\s*")
with open(target_file) as f:
    for line in f:
        match = version_pattern.match(line)
        if match:
            version = match.group(1)
            break

setup(
    name="ray_yarn",
    license="Apache 2.0",
    packages=find_packages(where=".", include=("ray_yarn*")),
    package_data={'': ['yarn.yaml']},
    include_package_data=True,
    python_requires=">=3.7",
    version=version,
    author="Ray Team",
    description="Deploy ray clusters on Apache YARN",
    long_description=long_description,
    url="https://github.com/ray-project/ray_yarn",
    install_requires=install_requires,
    zip_safe=False)
