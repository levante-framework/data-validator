import re
import math


# Utility function for converting dictionaries to snake_case and handling NaN values
def process_doc_dict(doc_dict, ignore_keys=None):
    # Convert camelCase to snake_case and handle NaN values
    if ignore_keys is None:
        ignore_keys = []
    converted_doc_dict = {camel_to_snake(key): handle_nan(value) for key, value in doc_dict.items()
                          if key not in ignore_keys}
    return converted_doc_dict


def camel_to_snake(camel_str):
    # Find all matches where a lowercase letter is followed by an uppercase letter
    matches = re.finditer(r'([a-z])([A-Z])', camel_str)

    # Insert an underscore before each uppercase letter
    for match in matches:
        camel_str = camel_str.replace(match.group(), match.group(1) + '_' + match.group(2))

    # Convert the entire string to lowercase
    snake_str = camel_str.lower()

    return snake_str


def handle_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    elif isinstance(value, dict):
        # Recursively handle NaN values in nested dictionaries
        return {key: handle_nan(val) for key, val in value.items()}
    return value
