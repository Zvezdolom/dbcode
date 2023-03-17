from setuptools import setup, find_packages

NAME = 'dbcode'
VERSION = '0.1'
DESCRIPTION = 'dbcode'
LONG_DESCRIPTION = 'dbcode'

# Setting up
setup(
    name=NAME,
    version=VERSION,
    author="Vladimir N. Kalinin",
    author_email="<vkalininz@mail.ru>",
    description=DESCRIPTION,
    packages=find_packages(),
    install_requires=['psycopg2', 'psycopg2-binary'],
    keywords=['python', 'database', 'sqlite3', 'postgresql', 'psycopg2'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Microsoft :: Windows",
    ]
)
