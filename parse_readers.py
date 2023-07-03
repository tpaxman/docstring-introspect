import re
from textwrap import dedent
from typing import Callable
import pandas as pd
import inspect

def extract_function_data(function: Callable) -> dict:
    """Return function docstring and parameters info."""
    docstring = dedent(function.__doc__)
    signature = inspect.signature(function)
    parameters = signature.parameters
    parameters_data = {
        parameter_name: extract_parameter_data(parameter)
        for parameter_name, parameter in parameters.items()
    }
    function_data = {
        'docstring': docstring,
        'parameters_data': parameters_data,
    }
    return function_data


def extract_parameter_data(parameter: inspect.Parameter) -> dict:
    """Return parameter data."""
    annotation = parameter.annotation
    default = parameter.default
    parameters_data = {
        'annotation': annotation,
        'default': default
    }
    return parameters_data


def extract_parameters_section(docstring: str) -> list:
    docstring_parameters_section = (
        re.search(
            r'(?<=^Parameters\n-).*(?=Returns\n-)',
            docstring,
            flags=re.MULTILINE | re.DOTALL
        )
        .group(0)
        .strip('-')
        .strip()
    )
    return docstring_parameters_section
    

def split_docstring_parameters_section(docstring_parameters_section: str) -> list:
    parameter_descriptions_raw = []
    dp = docstring_parameters_section
    while len(dp) > 0:
        result = re.search(r'^\S.*?(?=^\S)', dp, flags=re.DOTALL | re.MULTILINE)
        found_string = result.group(0) if result else dp
        parameter_descriptions_raw.append(found_string.strip())
        dp = dp.replace(found_string, '').strip()
    return parameter_descriptions_raw


def extract_parameter_descriptions(docstring: str) -> dict:
    docstring_parameters_section = extract_parameters_section(docstring)
    parameter_descriptions_raw = split_docstring_parameters_section(
        docstring_parameters_section
    )
    parameter_descriptions = {}
    for raw_parameter_description in parameter_descriptions_raw:
        name = re.match(r'^\**(\S+)', raw_parameter_description).group(1)
        if (result := re.search(r'^\**\S+\ : (.*)', raw_parameter_description)):
            accepted_values = result.group(1)
        else:
            accepted_values = ''
        description = '\n'.join(y.strip() for y in raw_parameter_description.split('\n')[1:])
        parameter_descriptions[name] = {
            'accepted_values': accepted_values,
            'description': description,
        }
    return parameter_descriptions


# PARSE

pandas_reader_functions = {
    function_name: getattr(pd, function_name) 
    for function_name in dir(pd)
    if 'read_' in function_name
}

functions_data = {
    function_name: extract_function_data(function)
    for function_name, function in pandas_reader_functions.items()
}

df_raw = pd.DataFrame(functions_data).T.rename_axis('function_name')
df_docstrings = df_raw['docstring'].reset_index()
df_parameter_signatures = (
    df_raw['parameters_data']
    .apply(pd.Series)
    .stack()
    .rename_axis(['function_name', 'parameter_name'])
    .apply(pd.Series)
    .reset_index()
)

df_docstring_parameter_descriptions = (
    df_docstrings
    .assign(parameter_description=lambda t: t.docstring.apply(
        extract_parameter_descriptions
    ))
    .set_index('function_name')
    ['parameter_description']
    .apply(pd.Series)
    .stack()
    .rename_axis(['function_name', 'parameter_name'])
    .apply(pd.Series)
    .reset_index()
)

df_parameters = pd.merge(
    df_parameter_signatures,
    df_docstring_parameter_descriptions,
    on=['function_name', 'parameter_name'],
    how='inner'
)

df_parameters.to_csv('reader_function_parameters.csv', index=False)


# INVESTIGATE

shared_descriptions = df_parameters.groupby(['parameter_name', 'description'])['function_name'].agg(set)

shared_annotations = df_parameters.groupby(['parameter_name', 'annotation'])['function_name'].agg(set)

shared_annotations_and_descrips = (
    df_parameters
    .groupby(['parameter_name', 'annotation', 'description'])
    ['function_name']
    .agg(set)
)

num_functions = (
    df_parameters
    .groupby('parameter_name')
    ['function_name']
    .agg(set)
    .reset_index()
    .assign(num_functions=lambda t: t.function_name.str.len())
    .sort_values(['num_functions', 'parameter_name'], ascending=[False, True])
    .reset_index(drop=True)
)
