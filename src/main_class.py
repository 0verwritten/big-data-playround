from .extraction.loaders import Loaders
from .business import *
from .transformation.statistics import Statistics


class DataAnalysis(Loaders, Statistics, Filters, Grouping, Joins, Processor, Windows):
    pass
