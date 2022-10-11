from setuptools import find_packages, setup
from data_engineer_pet_project_2 import __version__


def readme():
    with open('README.md') as f:
        return f.read()


def requires():
    with open('./requirements.txt') as f:
        return f.read().splitlines()


setup(
    name='data_engineer_pet_project_2',
    packages=find_packages(exclude=['test', 'test.*', '__app__*']),
    setup_requires=['wheel'],
    package_dir={'data_engineer_pet_project_2': 'data_engineer_pet_project_2'},
    package_data={'data_engineer_pet_project_2': ['config/files/*.yml']},
    install_requires=requires(),
    version=__version__,
    description='This project is an exemplary scenario designed to test your data engineering skills',
    long_description=readme(),
    author='Karim Safiullin',
    author_email='st.herbinar@gmail.com',
    python_requires='>=3',
    zip_safe=True,
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development',
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.8.9',
        'Environment :: Console',
    ],
)