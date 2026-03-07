from setuptools import setup, find_namespace_packages

version = "0.0.1"

setup(
    name="metaflow-nomad",
    version=version,
    description="HashiCorp Nomad extension for Metaflow",
    long_description=open("README.md").read(),
    author="Mustafa Hussain",
    author_email="",
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    python_requires=">=3.8",
    install_requires=["python-nomad>=2.0.0"],
)
