#!/usr/bin/env python

from setuptools import setup, find_packages, Command
from distutils.command.clean import clean
import os
import shutil

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.rst")) as f:
    long_description = f.read()


def grpc_files(pattern):
    for file_name in os.listdir("vda/grpc"):
        if pattern in file_name:
            file_path = os.path.join("vda/grpc", file_name)
            yield file_path


class Clean(clean):

    normal_list = [
        "dist",
        "vda.egg-info",
    ]

    force_list = [
        ".eggs",
        ".coverage",
        ".tox"
    ]

    def run(self):
        super(Clean, self).run()
        self._clean_file_or_dir(self.normal_list)
        for dir_name in ["vda", "tests"]:
            dir_path = os.path.join(here, dir_name)
            self._clean_cache(dir_path)
        for gen_file in grpc_files("pb2"):
            os.remove(gen_file)
        if self.all:
            self._clean_file_or_dir(self.force_list)

    def _clean_file_or_dir(self, inp_list):
        for inp in inp_list:
            full_path = os.path.join(here, inp)
            if os.path.isdir(full_path):
                shutil.rmtree(full_path)
            elif os.path.isfile(full_path):
                os.remove(full_path)

    def _clean_cache(self, root_name):
        for dir_name, subdir_list, file_list in os.walk(root_name):
            if dir_name.endswith("/__pycache__"):
                shutil.rmtree(dir_name)


class Grpc(Command):

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from grpc_tools import protoc
        for proto_path in grpc_files(".proto"):
            protoc.main((
                "",
                "--proto_path=.",
                "--python_out=.",
                "--grpc_python_out=.",
                proto_path,
            ))


setup(
    name="vda",
    version="0.0.4",
    description="virtual disk array",
    long_description=long_description,
    url="https://github.com/yupeng0921/vda",
    author="peng yu",
    author_email="yupeng0921@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    keywords="storage",
    python_requires=">=3.6",
    setup_requires=[
        "flake8",
        "grpcio-tools",
    ],
    install_requires=[
        "SQLAlchemy",
        "grpcio",
        "protobuf",
        "marshmallow",
    ],
    packages=find_packages(exclude=["tests*"]),
    entry_points={
        "console_scripts": [
            "vda_monitor=vda.monitor:main",
            "vda_portal=vda.portal:main",
            "vda_dn_agent=vda.dn_agent:main",
            "vda_cn_agent=vda.cn_agent:main",
            "vda_cli=vda.client.cli:main",
            "vda_db=vda.database:main",
        ]
    },
    cmdclass={
        "clean": Clean,
        "grpc": Grpc,
    },
)
