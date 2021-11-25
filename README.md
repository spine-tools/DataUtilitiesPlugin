# Data utilities plugin

This is a [Spine Toolbox](https://github.com/Spine-project/Spine-Toolbox) plugin
that contains utilities for Spine databases.
Install the plugin by choosing *Plugins -> Install plugin...* from Toolbox menu
and selecting *DataUtilitiesPlugin* from the dialog.

The plugin adds three Tool specifications to the main toolbar: **Validator** and **Atlite time series import** and **Interpolate missing time series values**

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


# Atlite time series import

[Atlite](https://atlite.readthedocs.io/en/latest/) importer can be used
to import energy systems converted time series from netCDF files
into object parameter values in Spine database.
It comes as a Tool specification and accompanying Python script.

The script requires [`xarray`](http://xarray.pydata.org/en/stable/) and
[`netcdf4`](http://unidata.github.io/netcdf4-python/) Python packages.

## Usage

Atlite time series import tool expects the following command line arguments:

- *Optional*: `-a <alternative name>` imports the time series into given alternative.
  Defaults to `Base`.
- Object class name
- Parameter name
- One or more paths to netCDF files
- Target database URL

The tool expects to find a dataset in each of the input files that contains two-dimensional data arrays.
The first dimension must be `time` and its coordinates the time stamps.
The second dimension is used as object names, and thus it must contain strings that can be used as such.

To set up the importer, create a Tool project item with *Atlite time series import* as its specification.
Next, specify target object class, parameter and, optionally, alternative as command line arguments to the tool.
A Data Connection can be used to provide the paths to netCDF input files
while a Data store provides a URL.

# Interpolate missing time series values

This plugin 'repairs' variable resolution time series data by interpolating missing data points
and converting the time series to fixed resolution.
The plugin updates the databases in-place replacing the original time series.

## Usage

The plugin Tool takes interpolation type as its first argument
and one or more database URLs as the rest.
The following interpolation types are available:

- `nearest` fills missing values by the nearest value
- `next` uses the next available value
- `previous` uses the value just before the gap
- `linear` is linear interpolation

# Version history

- 0.3.0: Added plugin that interpolates missing values in time series.
- 0.2.0: Added Atlite importer.
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
