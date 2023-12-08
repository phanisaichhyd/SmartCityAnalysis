# Databricks notebook source
from abc import ABC, abstractmethod


class Idata_ops(ABC):
    '''
    Creates an interface to impliment Data Operations Class
    '''
    @abstractmethod
    def read_data():
        pass

    @abstractmethod
    def write_data():
        pass

class Iops_factory(ABC):
    '''
    Interface to generate required objects
    '''
    @abstractmethod
    def gen_data_ops_obj(self):
        pass
