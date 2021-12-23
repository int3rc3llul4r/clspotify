import pathlib
from setuptools import setup
import setuptools



# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="clspotify",
    version="0.5.3",
    description="A spotify downloader.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/agent255/clspotify.git",
    author="hr",
    author_email="hemagna.rao@gmail.com",
    license="GPLv3",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3.9",
    ],
    packages=['zspotify'],
    install_requires=['ffmpy', 'music_tag', 'Pillow', 'protobuf', 'tabulate', 'tqdm',
                      'librespot @ https://github.com/kokarare1212/librespot-python/archive/refs/heads/rewrite.zip'],
    include_package_data=True,
)
