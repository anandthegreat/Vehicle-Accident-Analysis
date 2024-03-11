import yaml

def parse_yaml(path):
    """
    Parse config file as a dictionary
    :param path: path of the config file
    :return: dictionary of configuration
    """
    with open(path,'r') as f:
        return yaml.load(f, Loader=yaml.SafeLoader)

def read_csv_to_df(spark, path):
    """
    Read csv file to a spark dataframe
    :param spark: spark session instance
    :param path: path of the csv file
    :return: spark dataframe
    """
    df = spark.read.format('csv') \
        .option('header', 'true') \
        .option('inferschema', 'true') \
        .option('mode', 'PERMISSIVE') \
        .load(path)
    return df

def write_df_to_csv(df, path):
    """
    Write spark dataframe to a csv file
    :param df: spark df
    :param path: path of the csv file
    :return: None
    """
    # df.write.csv(path,header=True,mode='overwrite')
    df.toPandas().to_csv(path,index=False)

def printStep(func):
    def wrapper(*args, **kwargs):
        print(f"Running job {func.__name__} ")
        res = func(*args, **kwargs)
        return res
    return wrapper
