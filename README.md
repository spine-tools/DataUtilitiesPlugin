# Data utilities plugin

This is a [Spine Toolbox](https://github.com/Spine-project/Spine-Toolbox) plugin
that contains utilities Spine databases.
Install the plugin by choosing *Plugins -> Install plugin...* from Toolbox menu
and selecting *DataUtilitiesPlugin* from the dialog that opens.

The plugin adds **Validator** Tool specification to the main toolbar.

# Validator

Validator contains a Tool specification and corresponding Python script
that provide Spine data validation capabilities.

Currently, the plugin supports validating object and relationship parameter values.

The plugin depends on [``cerberus``](https://docs.python-cerberus.org) Python module
which must be available in Validator's Python environment.

## Usage

The Validator Tool specification can be used to create new validator tools.
The Tool expects the following command line arguments:

- Path to settings file
- One or more database URLs

Easiest way to provide the arguments is to connect one or more Data Stores
and a Data Connection which refers to the settings file to Validator.
The file path and URLs can be dragged and dropped into the command line arguments list
in Validator's properties tab.

## Settings file

Validator's settings are stored in a text file in JSON format.
The file must contain a JSON dict with two optional keys,
``"object_parameter_value"`` and ``"relationship_parameter_value"``.
The first key must map to a list of object parameter value validation *rules*
while the second must map to an equivalent list for relationships.

The template settings file below could serve as a starting point for defining validator rules.
The rules are explained in more detail after the template.

```json
{
  "object_parameter_value": [
    {
      "class": "class",
      "parameter": "param",
      "object": "",
      "alternative": "",
      "rule": {"type": "float", "max": 0.0}
    }
  ],
  "relationship_parameter_value": [
    {
      "class": "class",
      "parameter": "param",
      "objects": [""],
      "alternative": "",
      "rule": {"type": "float", "max": 0.0}
    }
  ]
}
```

Rules for object parameter values are JSON dicts and look like the following:

```json
{
  "_description": "template for object parameter value validation rules",
  "_comment": "fields starting with _ are ignored and can be used e.g. for comments",
  "class": "<object class name, optional>",
  "parameter": "<parameter name, optional>",
  "object": "<object name, optional>",
  "alternative": "<alternative name, optional>",
  "rule": {}
}
```

Rules for relationship parameter values look similar except for the ``"objects"`` list:

```json
{
  "class": "<relationship class name, optional>",
  "parameter": "<parameter name, optional>",
  "objects": ["<1st object name>", "<2nd object name>", "..."],
  "alternative": "<alternative name, optional>",
  "rule": {}
}
```

Note that entries in the ``"objects"`` list must match the relationship's dimensions.

The name fields in fact use the
[regular expression](https://docs.python.org/3.7/library/re.html#regular-expression-syntax)
syntax allowing flexible matching.
An empty regular expression will be used for matching if an optional name field is omitted.

The ``"rule"`` dict is the actual rule the matched parameter values are validated against.
It contains entries as documented in the
[Cerberus documentation](https://docs.python-cerberus.org/en/stable/validation-rules.html)
with few Spine data specific extensions.
Useful rules might be ``{"type": "number", "min": 0}``
or ``{"type": "map", "number of indexes": 3}``.

Additional types for the
[type](https://docs.python-cerberus.org/en/stable/validation-rules.html#type)
rule supported by Validator: ``"array"``, ``"duration"``,``"map"``, ``"time pattern"``, ``"time series"``

Validator has some additional rules too to validate the number of indexes of an indexed value.
They are tabulated below.

| Rule              | Usage                        |
|-------------------|------------------------------|
| min indexes       | ``{"min indexes": 1}``       |
| number of indexes | ``{"number of indexes": 3}`` |
| max indexes       | ``{"max indexes": 2}``       |


## Version history

- 0.1.1: Improved Validator logging.
- 0.1.0: First release. Includes Validator.

&nbsp;
<hr>
<center>
<table width=500px frame="none">
<tr>
<td valign="middle" width=100px>
<img src=https://europa.eu/european-union/sites/europaeu/files/docs/body/flag_yellow_low.jpg alt="EU emblem" width=100%></td>
<td valign="middle">This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 774629.</td>
</table>
</center>
