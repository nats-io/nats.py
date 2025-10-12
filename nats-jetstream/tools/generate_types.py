#!/usr/bin/env python3
"""Generate Python types from JSON schema definitions."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

SCHEMA_DIR = Path(__file__).parent.parent / "schemas" / "jetstream" / "api" / "v1"
OUTPUT_PATH = Path(__file__).parent.parent / "src/nats/jetstream/api/types.py"

# Types to skip generating (simple type aliases only)
SKIP_TYPES = [
    "golang_duration_nanos",
    "golang_int",
    "golang_uint64",
    "golang_int32",
    "golang_int64",
    "golang_time",
    "basic_name",
]

# Map schema type names to desired Python type names
TYPE_NAME_MAPPINGS = {
    "golang_duration_nanos": "int",
    "golang_int": "int",
    "golang_uint64": "int",
    "golang_int32": "int",
    "golang_int64": "int",
    "golang_time": "int",
    "basic_name": "str",
    "api_error": "Error",
    "stream_configuration": "StreamConfig",
    "stream_template_configuration": "StreamTemplateConfig",
    "consumer_configuration": "ConsumerConfig",
    "account_stats": "AccountInfo",
    "pub_ack_response": "PublishAck",
}


def get_type_name(schema_name: str) -> str:
    """Get the appropriate Python type name for a schema name.

    Args:
        schema_name: Original name from the schema (usually snake_case)

    Returns:
        The Python type name to use (either from mappings or converted to PascalCase)
    """
    if schema_name in TYPE_NAME_MAPPINGS:
        return TYPE_NAME_MAPPINGS[schema_name]

    return "".join(word.title() for word in schema_name.split("_"))


def load_schema(path: Path) -> dict[str, Any]:
    """Load JSON schema from file.

    Args:
        path: Path to schema file.

    Returns:
        The parsed JSON schema.
    """
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error loading schema from {path}: {e}", file=sys.stderr)
        sys.exit(1)


def resolve_ref(ref: str, definitions: dict[str, Any]) -> dict[str, Any]:
    """Resolve a JSON schema reference to its definition.

    Args:
        ref: Reference string (e.g. "#/definitions/type_name")
        definitions: Dictionary of all available definitions

    Returns:
        The resolved schema
    """
    if "#/definitions/" in ref or "definitions.json#/definitions/" in ref:
        type_name = ref.split("/")[-1]
        return definitions.get(type_name, {})
    return {}


def is_simple_type(schema: dict[str, Any], definitions: dict[str, Any] = None) -> bool:
    """Check if a schema represents a simple type (just a type definition).

    Args:
        schema: JSON schema object
        definitions: Dictionary of all available definitions for resolving references

    Returns:
        True if the schema is just a type definition, False otherwise
    """
    if "$ref" in schema and definitions is not None:
        ref_schema = resolve_ref(schema["$ref"], definitions)
        if ref_schema:
            return is_simple_type(ref_schema, definitions)

    allowed_keys = {
        "type",
        "description",
        "minimum",
        "maximum",
        "$comment",
        "format",
        "pattern",
        "minLength",
        "maxLength",
        "$ref",
    }
    return "type" in schema and set(schema.keys()).issubset(allowed_keys)


def resolve_type_recursively(schema: dict[str, Any], definitions: dict[str, Any]) -> dict[str, Any]:
    """Recursively resolve a type definition through references.

    Args:
        schema: JSON schema object
        definitions: Dictionary of all available definitions

    Returns:
        The fully resolved schema
    """
    if "$ref" in schema:
        ref_schema = resolve_ref(schema["$ref"], definitions)
        if ref_schema:
            return resolve_type_recursively(ref_schema, definitions)
    return schema


def merge_schemas(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Merge two schemas, combining their properties and required fields.

    Args:
        base: Base schema to merge into
        overlay: Schema to overlay on top of base

    Returns:
        Merged schema
    """
    result = base.copy()

    # Merge properties
    if "properties" in overlay:
        if "properties" not in result:
            result["properties"] = {}
        result["properties"].update(overlay["properties"])

    # Merge required fields
    if "required" in overlay:
        if "required" not in result:
            result["required"] = []
        result["required"].extend(overlay["required"])

    return result


def get_combined_schema(schema: dict[str, Any], definitions: dict[str, Any]) -> dict[str, Any]:
    """Get the combined schema from allOf fields.

    Args:
        schema: Schema that may contain allOf
        definitions: Dictionary of all available definitions

    Returns:
        Combined schema from allOf fields merged with base schema
    """
    if "allOf" not in schema:
        return schema

    # Start with the base schema (excluding allOf)
    result: dict[str, Any] = {k: v for k, v in schema.items() if k != "allOf"}

    # Merge in each allOf item
    for subschema in schema["allOf"]:
        if "$ref" in subschema:
            ref_schema = resolve_ref(subschema["$ref"], definitions)
            if ref_schema:
                # Skip merging pure oneOf schemas (validation constraints we can't express in TypedDict)
                if ref_schema.keys() == {"oneOf"}:
                    continue
                result = merge_schemas(result, ref_schema)
        else:
            result = merge_schemas(result, subschema)

    return result


def format_annotation_from_schema(schema: dict, name: str = None) -> str:
    """Format a type annotation from a schema.

    Args:
        schema: The schema to format
        name: Optional name hint for the type

    Returns:
        The formatted type annotation
    """
    # Handle references first
    if "$ref" in schema:
        ref_name = schema["$ref"].split("/")[-1]
        return get_type_name(ref_name)

    # Handle special cases
    match schema:
        case {"const": value}:
            return f'Literal["{value}"]'

        case {"enum": values}:
            literals = [f'"{v}"' for v in values]
            return f"Literal[{', '.join(literals)}]"

        case {"oneOf": schemas}:
            return f"Union[{', '.join(format_annotation_from_schema(s) for s in schemas)}]"

        case {"anyOf": schemas}:
            return f"Union[{', '.join(format_annotation_from_schema(s) for s in schemas)}]"

        case {"type": "object", "patternProperties": pattern_props}:
            pattern = next(iter(pattern_props.keys()))
            if pattern == ".*":
                value_schema = pattern_props[pattern]
                value_type = format_annotation_from_schema(value_schema)
                return f"dict[str, {value_type}]"

        case {"type": "object", "properties": _}:
            return name

        case {"type": "object", "additionalProperties": props} if props is not False:
            if isinstance(props, dict):
                value_type = format_annotation_from_schema(props)
                return f"dict[str, {value_type}]"
            return "dict"

        case {"type": "array", "items": items_schema}:
            items_type = format_annotation_from_schema(items_schema)
            if schema.get("nullable", False):
                return f"Union[list[{items_type}], None]"
            return f"list[{items_type}]"

        case {"type": "integer"}:
            return "int"

        case {"type": "number"}:
            return "float"

        case {"type": "string"}:
            return "str"

        case {"type": "boolean"}:
            return "bool"

        case _:
            return "Any"

    # Explicit return for safety (should never reach here)
    return "Any"


def generate_typed_dict_class_from_schema(name: str, schema: dict[str, Any], definitions: dict[str, Any]) -> list[str]:
    """Generate a TypedDict class from a schema.

    Args:
        name: Name of the class to generate
        schema: JSON schema object
        definitions: Dictionary of all available definitions

    Returns:
        List of lines defining the TypedDict class
    """
    lines = []

    lines.append(f"class {name}(TypedDict):")

    if "description" in schema:
        desc = schema["description"].strip()
        if desc:
            lines.append(f'    """{desc}"""')
            lines.append("")

    properties = schema.get("properties", {})
    required = set(schema.get("required", []))

    if not properties:
        lines.append("    pass")
        lines.append("")
        return lines

    for prop_name, prop_schema in sorted(properties.items()):
        type_annotation = format_annotation_from_schema(prop_schema, prop_name)

        if prop_name not in required:
            type_annotation = f"NotRequired[{type_annotation}]"

        lines.append(f"    {prop_name}: {type_annotation}")

        if "description" in prop_schema:
            desc = prop_schema["description"].strip()
            if desc:
                lines.append(f'    """{desc}"""')

        lines.append("")

    lines.append("")
    return lines


def generate_literal_alias_from_schema(name: str, schema: dict[str, Any]) -> list[str]:
    """Generate a type alias for a Literal type.

    Args:
        name: Name of the type alias
        schema: JSON schema object with enum values

    Returns:
        List of lines defining the type alias
    """
    if "enum" not in schema:
        return []

    values = [repr(v) for v in schema["enum"]]
    lines = [f"{name} = Literal[{', '.join(values)}]", ""]
    return lines


def generate_union_alias_from_schema(name: str, schema: dict[str, Any], definitions: dict[str, Any]) -> list[str]:
    """Generate a type alias for a Union type.

    Args:
        name: Name of the type alias
        schema: JSON schema object with oneOf or multiple types
        definitions: Dictionary of all available definitions

    Returns:
        List of lines defining the type alias
    """
    annotation = format_annotation_from_schema(schema, definitions)
    lines = [f"{name} = {annotation}", ""]
    return lines


def generate_type_from_schema(name: str, schema: dict[str, Any], definitions: dict[str, Any]) -> list[str]:
    """Generate a Python type from a JSON schema.

    Args:
        name: Name of the type to generate
        schema: JSON schema object
        definitions: Dictionary of all available definitions

    Returns:
        List of lines defining the type
    """
    # First, handle allOf by combining schemas
    if "allOf" in schema:
        schema = get_combined_schema(schema, definitions)

    if "oneOf" in schema:
        # Check if this is an error response union (oneOf with error_response variant)
        # In these cases, the client code handles errors separately, so we only generate the success type
        has_error_variant = False
        non_error_variants = []

        for variant in schema["oneOf"]:
            if "$ref" in variant:
                ref_name = variant["$ref"].split("/")[-1]
                if ref_name == "error_response":
                    has_error_variant = True
                else:
                    non_error_variants.append(variant)
            else:
                non_error_variants.append(variant)

        # If this is an error response union with exactly one non-error variant,
        # generate only the success variant as a TypedDict
        if has_error_variant and len(non_error_variants) == 1:
            variant = non_error_variants[0]

            # Merge base properties with the variant
            base_props = schema.get("properties", {})
            base_required = schema.get("required", [])

            if "$ref" in variant:
                ref_schema = resolve_ref(variant["$ref"], definitions)
                if ref_schema:
                    variant_props = ref_schema.get("properties", {})
                    variant_required = ref_schema.get("required", [])
                else:
                    variant_props = {}
                    variant_required = []
            else:
                variant_props = variant.get("properties", {})
                variant_required = variant.get("required", [])

            combined_props = base_props.copy()
            combined_props.update(variant_props)

            combined_required = list(base_required)
            combined_required.extend(variant_required)

            success_schema = {
                "type": "object",
                "properties": combined_props,
                "required": combined_required,
                "description": schema.get("description", ""),
            }

            return generate_typed_dict_class_from_schema(name, success_schema, definitions)

    if "oneOf" in schema:
        # Get common fields from properties
        base_schema = {
            "type": "object",
            "properties": schema.get("properties", {}),
            "required": schema.get("required", []),
            "description": schema.get("description", ""),
        }

        # Generate a type for each variant
        lines = []
        variant_types = []

        for i, variant in enumerate(schema["oneOf"]):
            if "$ref" in variant:
                ref_type = variant["$ref"].split("/")[-1]
                ref_name = get_type_name(ref_type)
                variant_name = f"{name}_{ref_name}"

                # Create a new schema that combines the base fields with the referenced type
                ref_schema = resolve_ref(variant["$ref"], definitions)
                if ref_schema:
                    # Merge properties
                    combined_props = base_schema["properties"].copy()
                    combined_props.update(ref_schema.get("properties", {}))

                    # Merge required fields
                    combined_required = list(base_schema["required"])
                    combined_required.extend(ref_schema.get("required", []))

                    combined = {
                        "type": "object",
                        "properties": combined_props,
                        "required": combined_required,
                        "description": base_schema["description"],
                    }

                    lines.extend(generate_typed_dict_class_from_schema(variant_name, combined, definitions))
                    variant_types.append(variant_name)
            else:
                # Handle inline object definitions
                variant_name = f"{name}_Variant{i}"

                # Merge properties
                combined_props = base_schema["properties"].copy()
                combined_props.update(variant.get("properties", {}))

                # Merge required fields
                combined_required = list(base_schema["required"])
                combined_required.extend(variant.get("required", []))

                combined = {
                    "type": "object",
                    "properties": combined_props,
                    "required": combined_required,
                    "description": base_schema["description"],
                }

                lines.extend(generate_typed_dict_class_from_schema(variant_name, combined, definitions))
                variant_types.append(variant_name)

        # Create the union type
        lines.extend([f"{name} = Union[{', '.join(variant_types)}]", ""])
        return lines
    elif ("type" in schema and schema["type"] == "object") or ("properties" in schema and "required" in schema):
        return generate_typed_dict_class_from_schema(name, schema, definitions)
    elif "enum" in schema:
        return generate_literal_alias_from_schema(name, schema)
    elif isinstance(schema.get("type"), list):
        return generate_union_alias_from_schema(name, schema, definitions)
    else:
        # For simple types, just create a type alias
        annotation = format_annotation_from_schema(schema, definitions)
        return [f"{name} = {annotation}", ""]


def get_type_dependencies(schema: dict[str, Any], definitions: dict[str, Any]) -> set[str]:
    """Get all type names that this schema depends on.

    Args:
        schema: JSON schema object
        definitions: Dictionary of all available definitions

    Returns:
        Set of type names this schema depends on
    """
    deps = set()

    if "$ref" in schema:
        ref_type = schema["$ref"].split("/")[-1]
        deps.add(get_type_name(ref_type))
    elif "allOf" in schema and "oneOf" in schema:
        # Get dependencies from allOf
        base_schema = get_combined_schema({"allOf": schema["allOf"]}, definitions)
        deps.update(get_type_dependencies(base_schema, definitions))

        # Get dependencies from each oneOf variant
        for variant in schema["oneOf"]:
            if "$ref" in variant:
                ref_type = variant["$ref"].split("/")[-1]
                deps.add(get_type_name(ref_type))
            else:
                combined = merge_schemas(base_schema, variant)
                deps.update(get_type_dependencies(combined, definitions))
    elif "allOf" in schema:
        combined = get_combined_schema(schema, definitions)
        deps.update(get_type_dependencies(combined, definitions))
    elif "oneOf" in schema:
        for subschema in schema["oneOf"]:
            deps.update(get_type_dependencies(subschema, definitions))
    elif "type" in schema and schema["type"] == "object":
        if "properties" in schema:
            for prop in schema["properties"].values():
                deps.update(get_type_dependencies(prop, definitions))
        if "allOf" in schema:
            for subschema in schema["allOf"]:
                deps.update(get_type_dependencies(subschema, definitions))
    elif "type" in schema and schema["type"] == "array":
        if "items" in schema:
            item_schema = schema["items"]
            deps.update(get_type_dependencies(item_schema, definitions))
            # If the array items can be null, we still want to capture the item type dependencies
            if "$ref" in item_schema:
                ref_type = item_schema["$ref"].split("/")[-1]
                deps.add(get_type_name(ref_type))
    elif isinstance(schema.get("type"), list):
        # For union types (type is a list), process each type
        for t in schema["type"]:
            if t != "null":  # Skip null type as it doesn't add dependencies
                type_schema = schema.copy()
                type_schema["type"] = t
                deps.update(get_type_dependencies(type_schema, definitions))

    return deps


def sort_types_by_dependencies(definitions: dict[str, Any]) -> list[str]:
    """Sort type names based on their dependencies.

    Args:
        definitions: Dictionary of all available definitions

    Returns:
        List of type names in dependency order
    """
    deps = {}
    for type_name, schema in definitions.items():
        deps[get_type_name(type_name)] = get_type_dependencies(schema, definitions)

    result = []
    visited = set()
    temp_visited = set()

    def visit(name: str):
        if name in temp_visited:
            return
        if name in visited:
            return

        temp_visited.add(name)
        for dep in deps.get(name, set()):
            visit(dep)
        temp_visited.remove(name)
        visited.add(name)
        result.append(name)

    for name in deps:
        if name not in visited:
            visit(name)

    return result


def main():
    """Main entry point."""
    # First, fetch all schemas and collect their definitions
    schemas = []
    definitions = {}
    generated_types = set()  # Track which types we've already generated

    for path in SCHEMA_DIR.glob("*.json"):
        print(f"Loading schema from {path}")
        schema = load_schema(path)

        # Collect schema if it has a title and any of: properties, required, or allOf
        if "title" in schema and ("properties" in schema or "required" in schema or "allOf" in schema):
            schemas.append(schema)

        # Add definitions from this schema
        if "definitions" in schema:
            definitions.update(schema["definitions"])

    print("\nAvailable types in schema:")
    for name in sorted(definitions.keys()):
        print(f"- {name}")

    lines = []

    # Add imports
    lines.append("from typing import Any, Literal, NotRequired, TypedDict, Union")
    lines.append("")
    lines.append("")

    # First generate base types in dependency order
    sorted_types = sort_types_by_dependencies(definitions)
    print("\nGenerating types in dependency order:")
    print(sorted_types)
    generated_count = 0
    skipped_count = 0

    for type_name in sorted_types:
        # Find original schema name
        original_name = None
        for name in definitions:
            if get_type_name(name) == type_name:
                original_name = name
                break

        if not original_name or original_name in SKIP_TYPES or type_name in generated_types:
            print(f"⚠️  Skipping type {original_name}")
            skipped_count += 1
            continue

        print(f"✓ Generating type for {original_name}")
        generated_count += 1
        schema = definitions[original_name]
        lines.extend(generate_type_from_schema(type_name, schema, definitions))
        generated_types.add(type_name)

    # Now process regular schemas
    for schema in schemas:
        # Get type name from title (e.g. "io.nats.jetstream.api.v1.account_info_response")
        original_name = schema["title"].split(".")[-1]  # Get last part after dot
        type_name = get_type_name(original_name)  # Convert to PascalCase if needed

        if type_name in generated_types:
            print(f"⚠️  Skipping duplicate type {original_name}")
            skipped_count += 1
            continue

        print(f"✓ Generating type for {original_name}")
        generated_count += 1
        lines.extend(generate_type_from_schema(type_name, schema, definitions))
        generated_types.add(type_name)

    print("\nSummary:")
    print(f"Total types in schema: {len(definitions) + len(schemas)}")
    print(f"Generated types: {generated_count}")
    print(f"Skipped types: {skipped_count}")
    print(f"Types in SKIP_TYPES: {len(SKIP_TYPES)}")

    # Write output file
    output = "\n".join(lines)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(output)
    print(f"\nWriting types to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
