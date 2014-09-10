try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

packages = [
    'periodic',
]

requires = ['periodic']

setup(
    name='periodic',
    version='0.0.1',
    description='The periodic task system client for python',
    author='Li Meng Jun',
    author_email='lmjubuntu@gmail.com',
    url='',
    packages=packages,
    package_dir={'periodic': 'periodic'},
    include_package_data=True,
    install_requires=requires,
)
