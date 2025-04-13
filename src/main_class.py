from .extraction.loaders import Loaders
from .business import *


class DataAnalysis(Loaders, Filters, Grouping, Joins, Processor, Windows):
    pass
