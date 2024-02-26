from enum import Enum


class GdsPropertyTypes(Enum):
    """
    Enumeration of supported property types inside the node/relationship schema of graphs to project into GDS sessions.
    """

    LONG = "long"
    DOUBLE = "double"
    LONG_ARRAY = "long[]"
    FLOAT_ARRAY = "float[]"
    DOUBLE_ARRAY = "double[]"
