from distutils.command.clean import clean
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="smapiconsumer",
    version="0.0.1",
    author="Nenad Veselinovic",
    author_email="nenves@gmail.com",
    description="Example package for consuming test api",
    long_description=long_description, 
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    project_urls={
        "Bug Tracker": "https://github.com/pypa/sampleproject/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
          'asyncio',
          'aiohttp',
          'aiofiles',
          'pandas'
      ]
)