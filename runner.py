import os
import sys

from pyspark.sql import SparkSession
from src.jobs import AccidentAnalysis
from src.utils import parse_yaml, write_df_to_csv
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == '__main__':

    spark = SparkSession.builder.appName('case_study').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    config = parse_yaml('config/config.yaml')
    accident_analysis = AccidentAnalysis(spark,config)

    df1 = accident_analysis.num_crash_male_died_gt2()
    df2 = accident_analysis.num_two_wheeler_booked()
    df3 = accident_analysis.get_top_veh_make_airbag_fail()
    df4 = accident_analysis.num_veh_valid_lic_hit_and_run()
    df5 = accident_analysis.get_state_max_crash_no_female()
    df6 = accident_analysis.get_top_veh_make_max_inj()
    df7 = accident_analysis.get_body_style_top_ethnic_grp()
    df8 = accident_analysis.get_top_zip_crash_with_alcohol()
    df9 = accident_analysis.num_crash_no_damage()
    df10 = accident_analysis.get_top_veh_make_speeding_offense()

    dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10]
    output_dir = config.get("OUTPUT_DIR")
    output_files = config.get("OUTPUT_FILES")

    for i, df in enumerate(dfs, start=1):
        print(f"Writing analytics {i} to csv...")
        path = output_dir + output_files.get(f"analytics_{i}")
        write_df_to_csv(df, path)

    spark.stop()


