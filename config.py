from __future__ import annotations

import configparser
from enum import Enum, auto
from typing import Any, Type, TypeVar

from pydantic import BaseModel

from daskapp.root import ROOT


class StrEnum(str, Enum):

    def _generate_next_value_(_name, _a, _b, _c):
        return str(_name)

    def __str__(self):
        return self.value


class ConfigSection(StrEnum):
    S3         = auto()
    EMR        = auto()
    DASK       = auto()
    DASH       = auto()
    SPACY      = auto()
    GENERAL    = auto()
    AWSACADEMY = auto()


class ClusterStatus(StrEnum):
    STARTING      = auto()
    BOOTSTRAPPING = auto()
    WAITING       = auto()
    TERMINATING   = auto()


class _ConfigHandler(BaseModel):
    _filename: str = ""
    _config: configparser.ConfigParser
    _section: ConfigSection

    def __setattr__(self, name: str, value: Any) -> None:
        if not name.startswith("_"):
            self._config[self._section][name] = str(value)
            with open(self._filename, 'w') as f:
                self._config.write(f)
        super().__setattr__(name, value)

    def __init_subclass__(cls):
        
        if getattr(_ConfigHandler, "_config", None) is None:
            _ConfigHandler._config = configparser.ConfigParser()
        _ConfigHandler._config.read(_ConfigHandler._filename)

        alias = cls.__name__.strip("Config").upper()
        cls._section = getattr(ConfigSection, alias)
        return cls

    @classmethod
    def from_ini(cls):
        return cls(**_ConfigHandler._config[cls._section])


class EmrConfig(_ConfigHandler):
    cluster_status: ClusterStatus = ClusterStatus.STARTING
    dashboard_link_file: str = 'dashboard_link.txt'
    cluster_id: str = ""
    dns: str = ""


class AwsAcademyConfig(_ConfigHandler):
    login: str
    password: str
    course_module:str
    course_item:str
    key:str
    secret:str
    token:str


class S3Config(_ConfigHandler):
    bucket: str
    assets_path: str
    raw_data_path: str
    raw_data_file: str
    output_path: str
    processed_file:str
    grams_path:str
    bootstrap_script: str
    cleaner_script: str
    cluster_init_script: str

    @staticmethod
    def from_s3(*args):
        return "s3://" + "/".join(args)

    @property
    def raw_file_path(self):
        return self.from_s3(self.bucket, self.raw_data_path, self.raw_data_file)
      
    @property
    def processed_file_path(self):
        return self.from_s3(self.bucket, self.output_path, self.processed_file)

    @property
    def output_path_whole(self):
        return self.from_s3(self.bucket, self.output_path)

    @property
    def unique_lemmata_path(self):
        return self.output_path_whole + "/unique_lemmata.csv"

    @property
    def hashtags_path(self):
        return self.output_path_whole + "/hashtags.csv"

    @property
    def lemmata_bigrams_path(self):
        return self.output_path_whole + "/bigrams.csv"

    @property
    def lemmata_trigrams_path(self):
        return self.output_path_whole + "/trigrams.csv"


class SpacyConfig(_ConfigHandler):
    model: str


class DaskConfig(_ConfigHandler):
    num_partitions: int
    dashboard_link: str
    client_link: str
    dashboard_port: str
    client_port: str


class DashConfig(_ConfigHandler):
    pass


class GeneralConfig(_ConfigHandler):
    stopwords:str
    assets_dir:str

    @property
    def stopwords_path(self):
        return ROOT + self.assets_dir + self.stopwords


class ConfigClasses(Enum):
    S3         = S3Config
    EMR        = EmrConfig
    DASK       = DaskConfig
    DASH       = DashConfig
    SPACY      = SpacyConfig
    GENERAL    = GeneralConfig
    AWSACADEMY = AwsAcademyConfig


class Config:
    file       : str
    s3         : S3Config
    emr        : EmrConfig
    spacy      : SpacyConfig
    dask       : DaskConfig
    dask       : DashConfig
    general    : GeneralConfig
    awsacademy : AwsAcademyConfig

    @classmethod
    def from_ini(
        cls, 
        file:str='config.ini'
    ) -> Config:

        cls.file = f"{ROOT}/{file}"

        _ConfigHandler._filename = cls.file
        _ConfigHandler._config = configparser.ConfigParser()
        _ConfigHandler._config.read(cls.file)

        for section_name in ConfigSection:
            section_class = getattr(ConfigClasses, section_name)
            setattr(cls,
                section_name.lower(),
                section_class.value.from_ini()
            )
        return cls


