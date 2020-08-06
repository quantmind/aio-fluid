import os
import sys

from setuptools import find_packages, setup

import fluid as pkg


def read(name):
    filename = os.path.join(os.path.dirname(__file__), name)
    with open(filename, encoding="utf8") as fp:
        return fp.read()


def requirements(name):
    install_requires = []
    dependency_links = []

    for line in read(name).split("\n"):
        if line.startswith("-e "):
            link = line[3:].strip()
            if link == ".":
                continue
            dependency_links.append(link)
            line = link.split("=")[1]
        line = line.strip()
        if line:
            install_requires.append(line)

    return install_requires, dependency_links


install_requires = requirements("dev/requirements.txt")[0]

if sys.version_info < (3, 7):
    install_requires.append("dataclasses")


meta = dict(
    version=pkg.__version__,
    description=pkg.__doc__,
    name="fluid",
    packages=find_packages(exclude=["tests", "tests.*"]),
    long_description=read("readme.md"),
    long_description_content_type="text/markdown",
    license="BSD",
    author_email="luca@quantmind.com",
    maintainer_email="luca@quantmind.com",
    url="https://github.com/quantmind/fluid",
    python_requires=">=3.8.0",
    install_requires=install_requires,
    include_package_data=True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: JavaScript",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
    ],
)


if __name__ == "__main__":
    setup(**meta)
