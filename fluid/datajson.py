from typing import Dict, Optional

from jsonschema import validate
from openapi.spec import SchemaParser


def validate_config(
    config: Dict, schema: Dict, defaults: bool = False
) -> Optional[Dict]:
    if schema:
        additional = schema.get("additionalProperties")
        schema["additionalProperties"] = True
        if defaults:
            config = _add_defaults(config, schema)
        validate(config, schema)
        if not additional:
            for key in tuple(config):
                if key not in schema["properties"]:
                    config.pop(key)
        return config


def json_meta_data(Dataclass: type, **kwargs) -> Dict:
    """Create a JSON payload with schema from a Dataclass"""
    parser = SchemaParser()
    parser.add_schema_to_parse(Dataclass)
    schemas = parser.parsed_schemas()
    schema = schemas.pop(Dataclass.__name__)
    if schemas:
        _replace_refs(schema, schemas)
    description = schema.pop("description")
    data = dict(description=description, schema=schema)
    data.update(kwargs)
    return data


# INTERNALS


def _add_defaults(config: Dict, schema: Dict) -> Dict:
    cfg = config.copy()
    for name, prop in schema["properties"].items():
        if name not in cfg and "default" in prop:
            cfg[name] = prop["default"]
    return cfg


def _replace_refs(schema, schemas):
    props = {}
    for name, field in schema["properties"].items():
        ref = field.get("$ref")
        is_array = False
        if not ref and field.get("type") == "array":
            is_array = True
            ref = field["items"].get("$ref")
        if ref and ref.startswith("#"):
            key = ref.split("/")[-1]
            if key in schemas:
                new_field = schemas[key]
                _replace_refs(schemas[key], schemas)
                if is_array:
                    field["items"] = new_field
                else:
                    description = field.get("description")
                    if description:
                        new_field["description"] = description
                    field = new_field
        props[name] = field
    schema["properties"] = props
