from enum import Enum

NODE_PROPERTY_SCHEMA = "nodePropertySchema"
RELATIONSHIP_PROPERTY_SCHEMA = "relationshipPropertySchema"


class GdsPropertyTypes(Enum):
    LONG = "long"
    DOUBLE = "double"
    LONG_ARRAY = "long[]"
    FLOAT_ARRAY = "float[]"
    DOUBLE_ARRAY = "double[]"
