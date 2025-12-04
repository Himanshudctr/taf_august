from src.utility.general_lib import read_schema,flatten


def read_file(config, spark, dir_path):
    """
    :param config: this will contain configs of source or target
    :param spark: spark session
    :return: spark dataframe
    """

    path = config['path']
    type = config['type'].lower()
    schema = config['schema']
    options = config['options']
    exclude = config['exclude_cols']

    if type == 'csv':
        if schema == 'Y':
            schema_json = read_schema(dir_path)       #*
            df = spark.read.schema(schema_json).csv(path=path, header=options['header'], sep=options['delimiter'])
        else:
            df = spark.read.csv(path=path, header=options['header'], sep=options['delimiter'],
                                inferSchema=options['inferSchema'])
    elif type == 'json':
        df = spark.read.json(path=path, multiLine=options['multiline'])
        if options['flatten'] == 'Y':
            df = flatten(df)
    elif type == 'parquet':
        df = spark.read.parquet(path)
    elif type == 'avro':
        df = spark.read.format('avro').load(path=path)
    elif type == 'txt':
        df = spark.read.format('csv').load(path=path, header=options['header'], sep=options['delimiter'])
    return df



"""
#After data read from json files check for Array type and Struct type
#if we have Array type column, Do explode/explode outer
#then check again schema if still array type columns present do explode again
#check if any struct type then extract each field from struct field

"""