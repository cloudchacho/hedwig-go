package jsonschema

var containerSchemaStr = `
{
    "id": "https://github.com/cloudchacho/hedwig-go/schemas#/format_schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Schema for Hedwig messages",
    "required": [
        "id",
        "schema",
        "format_version",
        "metadata",
        "data"
    ],
    "properties": {
        "id": {
            "type": "string",
            "description": "Message identifier",
            "minLength": 36,
            "maxLength": 36,
            "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        },
        "schema": {
            "type": "string",
            "description": "Schema to validate the data object with",
            "minLength": 1
        },
        "format_version": {
            "type": "string",
            "description": "Format version for the message",
            "enum": [
                "1.0"
            ]
        },
        "metadata": {
            "type": "object",
            "description": "Metadata associated with the message",
            "properties": {
                "publisher": {
                    "type": "string",
                    "description": "Message publisher service"
                },
                "timestamp": {
                    "type": "number",
                    "description": "Timestamp in epoch milliseconds (integer)",
                    "format": "int"
                },
                "headers": {
                    "type": "object",
                    "description": "Custom headers associated with the message"
                }
            },
            "additionalProperties": true
        },
        "data": {
            "type": "object",
            "description": "Message data"
        }
    },
    "additionalProperties": true
}
`
