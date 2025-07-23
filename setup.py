from setuptools import setup, find_packages

setup(
    name="cryptostreamx",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "requests>=2.26.0",
        "python-dotenv>=0.19.0",
        "pyyaml>=6.0.0",
        "pyspark>=3.3.0",
        "delta-spark>=1.2.0",
    ],
)
