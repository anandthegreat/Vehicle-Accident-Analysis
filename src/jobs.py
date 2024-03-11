from pyspark.sql import Window
from pyspark.sql.functions import count, col, desc, countDistinct, row_number, split, sum
from pyspark.sql.types import IntegerType

from src.utils import read_csv_to_df, printStep

class AccidentAnalysis:

    def __init__(self,spark,config):
        input_dir = config.get("INPUT_DIR")
        input_files = config.get("INPUT_FILES")
        self.spark = spark

        self.charges_df = read_csv_to_df(spark, input_dir + input_files.get('charges'))
        self.damages_df = read_csv_to_df(spark, input_dir + input_files.get('damages'))
        self.endorse_df = read_csv_to_df(spark, input_dir + input_files.get('endorse'))
        self.person_df = read_csv_to_df(spark, input_dir + input_files.get('primary_person'))
        self.restrict_df = read_csv_to_df(spark, input_dir + input_files.get('restrict'))
        self.units_df = read_csv_to_df(spark, input_dir + input_files.get('units'))

        self.person_join_units_df = self.person_df.join(self.units_df,['CRASH_ID','UNIT_NBR'])
        self.units_join_charges_df = self.units_df.join(self.charges_df, ['CRASH_ID', 'UNIT_NBR'])

    @printStep
    def num_crash_male_died_gt2(self):
        """
        Analytics 1: Number of accidents in which males killed are greater than 2
        :return: DataFrame
        """
        df = self.person_df.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('PRSN_INJRY_SEV_ID') == 'KILLED')) \
                .groupBy('CRASH_ID') \
                .agg(count('CRASH_ID').alias('crash_count')) \
                .filter(col('crash_count') > 2)
        return self.spark.createDataFrame([(df.count(),)], ["num_crash_male_died_gt2"])

    @printStep
    def num_two_wheeler_booked(self):
        """
        Analytics 2: Number of two wheelers booked for crashes
        :return: DataFrame
        """
        df = self.units_join_charges_df \
                .filter((col('VEH_BODY_STYL_ID') == 'MOTORCYCLE') &
                        (col('CHARGE').like('%CHARGE%')))
        return self.spark.createDataFrame([(df.count(),)], ["num_two_wheeler_booked"])

    @printStep
    def get_top_veh_make_airbag_fail(self):
        """
        Analytics 3: Top 5 Vehicle Makes of the cars present in the crashes
        in which driver died and Airbags did not deploy
        :return: DataFrame
        """
        df = self.person_join_units_df\
                .filter((col('PRSN_TYPE_ID') == 'DRIVER') &
                        (col('PRSN_INJRY_SEV_ID') == 'KILLED') &
                        (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED') &
                        (col('VEH_MAKE_ID') != 'NA') &
                        (~col('VEH_MAKE_ID').like('%OTHER%')))\
                .groupBy('VEH_MAKE_ID')\
                .count().withColumnRenamed("count","num_deadly_crashes")\
                .orderBy(desc('num_deadly_crashes'))\
                .limit(5)
        return df

    @printStep
    def num_veh_valid_lic_hit_and_run(self):
        """
        Analytics 4: number of Vehicles with driver having valid licences
        involved in hit and run
        :return: DataFrame
        """
        df = self.person_join_units_df\
                .filter((col('VEH_HNR_FL') == 'Y') &
                        (col('PRSN_TYPE_ID') == 'DRIVER')
                        & (~col('DRVR_LIC_TYPE_ID').isin(['NA','OTHER','UNKNOWN','UNLICENSED'])))
        return self.spark.createDataFrame([(df.count(),)], ["num_veh_valid_lic_hit_and_run"])

    @printStep
    def get_state_max_crash_no_female(self):
        """
        Analytics 5: state having highest number of accidents
        in which females are not involved
        :return: DataFrame
        """
        df = self.person_join_units_df\
                .filter((col('PRSN_GNDR_ID') != 'FEMALE') & (~col('DRVR_LIC_STATE_ID').isin(['NA','Other','Unknown'])))\
                .groupBy(col('DRVR_LIC_STATE_ID'))\
                .agg(countDistinct("CRASH_ID").alias('num_acc_no_female'))\
                .orderBy(desc('num_acc_no_female'))\
                .limit(1)
        return df

    @printStep
    def get_top_veh_make_max_inj(self):
        """
        Analytics 6: Top 3rd to 5th VEH_MAKE_IDs that contribute to
        a largest number of injuries including death
        :return: DataFrame
        """
        window = Window.orderBy(desc('num_inj_death'))
        df = self.units_df.filter((col('VEH_MAKE_ID') != 'NA') & (~self.units_df.VEH_MAKE_ID.like('%OTHER%'))) \
                .withColumn('TOT_INJRY_CNT', col('TOT_INJRY_CNT') + col('DEATH_CNT')) \
                .withColumn('TOT_INJRY_CNT', col('TOT_INJRY_CNT').cast(IntegerType())) \
                .groupBy(col('VEH_MAKE_ID')) \
                .agg(sum('TOT_INJRY_CNT').alias('num_inj_death')) \
                .withColumn('RN',row_number().over(window)) \
                .filter((col('RN') >= 3) & (col('RN') <= 5)) \
                .select(col('VEH_MAKE_ID').alias('top_3_to_5_inj_veh_make'))
        return df

    @printStep
    def get_body_style_top_ethnic_grp(self):
        """
        Analytics 7: For all the body styles involved in crashes,
        finds the top ethnic user group of each unique body style
        :return: DataFrame
        """
        window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('cnt'))
        df = self.person_join_units_df \
                .filter((~col('VEH_BODY_STYL_ID').isin(['NA', 'NOT REPORTED', 'UNKNOWN'])) &
                        (~col('PRSN_ETHNICITY_ID').isin(['NA', 'OTHER', 'UNKNOWN'])) &
                        (~self.units_df.VEH_BODY_STYL_ID.like('%OTHER%'))) \
                .groupBy(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']) \
                .agg(count('*').alias('cnt')) \
                .withColumn('RN', row_number().over(window)) \
                .filter(col('RN') == 1) \
                .select(col('VEH_BODY_STYL_ID').alias('body_style'), col('PRSN_ETHNICITY_ID').alias('top_ethnic_usr_grp'))
        return df

    @printStep
    def get_top_zip_crash_with_alcohol(self):
        """
        Analytics 8: Among the crashed cars, finds the Top 5 Zip Codes
        with highest number crashes with alcohols as the factor
        :return: DataFrame
        """
        df = self.person_join_units_df \
                .filter((col('PRSN_ALC_RSLT_ID') == 'Positive') &
                        ((self.units_df.VEH_BODY_STYL_ID.like('%CAR%')) | (col('VEH_BODY_STYL_ID') == 'SPORT UTILITY VEHICLE')) &
                        (col('DRVR_ZIP').isNotNull())) \
                .groupBy(col('DRVR_ZIP')) \
                .agg(count('*').alias('num_crashes')) \
                .orderBy(desc('num_crashes')) \
                .limit(5)
        return df

    @printStep
    def num_crash_no_damage(self):
        """
        Analytics 9: Distinct Crash IDs where No Damaged Property was observed
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        :return: DataFrame
        """
        df = self.units_df.join(self.damages_df, 'CRASH_ID', "left") \
            .filter((col('FIN_RESP_TYPE_ID').like('%INSURANCE%')) &
                    ((col('VEH_BODY_STYL_ID').like('%CAR%')) | (col('VEH_BODY_STYL_ID') == 'SPORT UTILITY VEHICLE')) &
                    ((col('DAMAGED_PROPERTY').like('%NONE%')) | (col('DAMAGED_PROPERTY').isNull())) &
                    ((split(col('VEH_DMAG_SCL_1_ID'), ' ')[1].cast('int') > 4) |
                     (split(col('VEH_DMAG_SCL_2_ID'), ' ')[1].cast('int') > 4))) \
            .agg(countDistinct('CRASH_ID').alias('unique_crash_ids'))
        return df

    @printStep
    def get_top_veh_make_speeding_offense(self):
        """
        Analytics 10: Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours
        and has car licensed with the Top 25 states with highest number of offences
        :return: DataFrame
        """
        top_10_veh_color = self.units_df.filter(col('VEH_COLOR_ID') != 'NA') \
            .groupBy('VEH_COLOR_ID') \
            .agg(count('*').alias('color_cnt')).orderBy(desc('color_cnt')).limit(10)

        top_25_state_highest_offences = self.units_df.filter(col('VEH_LIC_STATE_ID') != 'NA') \
            .groupBy('VEH_LIC_STATE_ID') \
            .agg(count('*').alias('offense_cnt')) \
            .orderBy(desc('offense_cnt')).limit(25)

        df = self.units_join_charges_df \
            .join(self.person_df, ['CRASH_ID', 'UNIT_NBR']) \
            .join(top_10_veh_color, ['VEH_COLOR_ID']) \
            .join(top_25_state_highest_offences, ['VEH_LIC_STATE_ID']) \
            .filter((col('CHARGE').like('%SPEED%')) &
                    (~col('DRVR_LIC_TYPE_ID').isin(['NA', 'OTHER', 'UNKNOWN', 'UNLICENSED']))) \
            .groupBy(col('VEH_MAKE_ID')) \
            .agg(count('*').alias('cnt')) \
            .orderBy(desc('cnt')) \
            .limit(5)
        return df