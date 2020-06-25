import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cffadb-GreyPaperclip", # Replace with your own username
    version="0.0.1",
    author="Richard Borrett",
    author_email="python@richardborrett.com",
    description="Prototype Casual Football Finance_Manager_backend",
    long_description=,
    long_description_content_type="text/markdown",
    url="https://github.com/GreyPaperclip/CFFADB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
