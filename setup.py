import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="pygeoapi-kubernetes-papermill",
    version="0.0.1",
    author="Bernhard Mallinger",
    author_email="bernhard.mallinger@eox.com",
    description="Run notebooks on a k8s cluster via pygeoapi",
    license="MIT",
    install_requires=["kubernetes", "scrapbook", "typed_json_dataclass==1.2.1"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eurodatacube/pygeoapi-kubernetes-papermill",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
