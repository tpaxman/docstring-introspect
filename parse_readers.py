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

# {
#     k: v.get('parameters_data').get('chunksize') 
#     for k, v in functions_data.items() 
#     if v.get('parameters_data').get('chunksize')
# }

# parameters_vs_functions = (
#     pd.DataFrame(
#         [
#             (function_name, list(data['parameters_data'].keys()))
#             for function_name, data in functions_data.items()
#         ],
#         columns=['function_name', 'parameter_name']
#     )
#     .explode('parameter_name')
#     .assign(dummy=1)
#     .set_index(['parameter_name','function_name'])['dummy']
#     .unstack()
#     .fillna(0)
#     .astype('int')
# )


# reader_functions = {x: getattr(pd, x) for x in public_reader_function_names}
# reader_signatures = {x: inspect.signature(x) for x in reader_functions}
# reader_parameters = {k: v.parameters for k, v in reader_signatures.items()}


# READER_FUNCTION_NAMES_FROM_GREP = {
#     "read_array",
#     "read_clipboard",
#     "read_column",
#     "read_coordinates",
#     "read_csv",
#     "read_csv_check_warnings",
#     "read_data",
#     "read_dta",
#     "read_excel",
#     "read_ext",
#     "read_ext_xlrd",
#     "read_feather",
#     "read_fwf",
#     "read_gbq",
#     "read_hdf",
#     "read_html",
#     "read_index",
#     "read_index_node",
#     "read_json",
#     "read_metadata",
#     "read_multi_index",
#     "read_orc",
#     "read_parquet",
#     "read_pickle",
#     "read_query",
#     "read_sas",
#     "read_spss",
#     "read_sql",
#     "read_sql_query",
#     "read_sql_table",
#     "read_stata",
#     "read_table",
#     "read_table_check_warnings",
#     "read_xml",
#     "read_xml_iterparse",
#     "read_xml_iterparse_comp",
# }


# private_readers = list(
#     set(READER_FUNCTION_NAMES_FROM_GREP).difference(public_readers)
# )

