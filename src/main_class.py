from .extraction.loaders import Loaders
from .export.writers import Export
from .business import *
from .transformation.statistics import Statistics


class DataAnalysis(Loaders, Statistics, Export, Processor):
    pass
