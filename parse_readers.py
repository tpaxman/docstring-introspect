import pandas as pd
import inspect

public_reader_function_names = [x for x in dir(pd) if 'read_' in x]

functions_data = {}
for function_name in public_reader_function_names:
    function = getattr(pd, function_name)
    signature = inspect.signature(function)
    docstring = function.__doc__

    parameters = signature.parameters
    parameters_data = {}
    for parameter_name, parameter in parameters.items():
        annotation = parameter.annotation
        default = parameter.default
        parameters_data[parameter_name] = {
            'annotation': annotation,
            'default': default
        }
    functions_data[function_name] = {
        'docstring': docstring,
        'parameters': parameters_data,
    }

parameters_vs_functions = (
    pd.DataFrame(
        [
            (function_name, list(data['parameters'].keys()))
            for function_name, data in functions_data.items()
        ],
        columns=['function_name', 'parameter_name']
    )
    .explode('parameter_name')
    .assign(dummy=1)
    .set_index(['parameter_name','function_name'])['dummy']
    .unstack()
    .fillna(0)
    .astype('int')
)

def extract_parameter_descriptions(docstring: str) -> list:
    docstring_params = (
        re.search(
            r'(?<=^Parameters\n-).*(?=Returns\n-)',
            docstring,
            flags=re.MULTILINE | re.DOTALL
        )
        .group(0)
        .strip('-')
        .strip()
    )

    parameter_descriptions_raw = []
    dp = docstring_params
    while len(dp) > 0:
        result = re.search(r'^\S.*?(?=^\S)', dp, flags=re.DOTALL | re.MULTILINE)
        found_string = result.group(0) if result else dp
        parameter_descriptions_raw.append(found_string.strip())
        dp = dp.replace(found_string, '').strip()

    parameter_descriptions = {}
    for x in parameter_descriptions_raw:
        name = re.match(r'^\w+', x).group(0)
        accepted_values = re.search(r'^\w+\ : (.*)', x).group(1)
        description = '\n'.join(y.strip() for y in x.split('\n')[1:])
        parameter_descriptions[name] = {
            'accepted_values': accepted_values,
            'description': description,
        }

    return parameter_descriptions

parameter_descriptions = extract_parameter_descriptions(

a = re.search(r'^\S.*?(?=^\S)', docstring_params, flags=re.DOTALL | re.MULTILINE)

{
    k: v.get('parameters').get('chunksize') 
    for k, v in functions_data.items() 
    if v.get('parameters').get('chunksize')
}



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

