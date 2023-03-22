from functools import reduce
import json
import sys
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType,ShortType, DecimalType
from pyspark.sql.types import IntegerType, BooleanType, DateType, ByteType 
from pyspark.sql.types import TimestampType
import boto3
import time
from datetime import datetime
import datetime
from pyspark.sql.types import StringType, LongType, ByteType, StringType
import pyspark.sql.functions as F
import argparse, sys

# setting log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

parser = argparse.ArgumentParser()
parser.add_argument("--edp_raw_zone_bucket", help="Missing required argument 'message1'")
parser.add_argument("--edp_data_product_bucket", help="Missing required argument 'message1'")
arser.add_argument("--tableName", help="Missing required argument 'message1'")
args = parser.parse_args()

edp_raw_zone_bucket = args.edp_raw_zone_bucket
edp_data_product_bucket = args.edp_data_product_bucket
tableName               = args.tableName

logger.info(f"Raw bucket name is: {edp_raw_zone_bucket}")
logger.info(f"Data Product bucket name is: {edp_data_product_bucket}")
logger.info(f"Table name: {tableName}")

#calculate prefix
def calculatePrefix():
    s3_client = boto3.client(service_name='s3')
    result = s3_client.get_object(Bucket=edp_raw_zone_bucket, Key='PROSPECT/acxiom/notification.txt') 
    for line in result["Body"].read().splitlines():
        prefix = line.decode('utf-8')
    print(prefix)
    return prefix

#Read Files from Raw Zone
def readRawZone(spark):
    print("Reading from Raw Zone...")
    bucketName = "s3://" + edp_raw_zone_bucket + "/PROSPECT/acxiom/" + calculatePrefix() + "/"
    print(bucketName)
    data_acxiom_df = spark.read.format("parquet").load(bucketName)
    return data_acxiom_df



# Add housekeeping columns to the table
def addhousekeepingColumns(data_acxiom):
    data_acxiom = (
        data_acxiom.withColumn("CRET_TS", F.current_timestamp())
        .withColumn("CHNL_CD", lit("ACXIOM"))
        .withColumn("CHNL_SRC_CD", lit("ACXIOM"))
        .withColumn("ADDR_LNK_KEY", col("INPUT_ABILITEC_ADDRESS_LINK_16BYTE"))
        .withColumn("HSHLD_LNK_KEY", col("INPUT_ABILITEC_ADDRESS_LINK_16BYTE"))
        .withColumn("CURR_RCD_IND", lit("Y"))
    )


# Add standard columns to the table
def addDefaultColumns(data_acxiom_poptn):
    data_acxiom_poptn = (
        data_acxiom_poptn.withColumn(
            "ACXIOM_POPTN_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return data_acxiom_poptn

# Prepare an age table
# Prepare an age table
def prepareAcxiomPoptnTable(data_acxiom_poptn):
    
    data_acxiom_poptn_upd = addDefaultColumns(data_acxiom_poptn)
    data_acxiom_poptn_hkc = addhousekeepingColumns(data_acxiom)
    
    data_acxiom_poptn_new = data_acxiom_poptn_upd.select(
        (data_acxiom_poptn_upd.ACXIOM_POPTN_DTL_KEY.cast(StringType())),
        (data_acxiom_poptn_hkc.CRET_TS.cast(TimestampType())),
        (data_acxiom_poptn_hkc.CHNL_CD.cast(StringType())),
        (data_acxiom_poptn_hkc.CHNL_SRC_CD.cast(StringType())),
        (data_acxiom_poptn_hkc.ADDR_LNK_KEY.cast(StringType())),
        (data_acxiom_poptn_hkc.HSHLD_LNK_KEY.cast(StringType())),
        (data_acxiom_poptn_hkc.CURR_RCD_IND.cast(StringType())),    
        (data_acxiom_poptn_upd.IBE1273_01.cast(StringType())),
        (data_acxiom_poptn_upd.IBE1273_02.cast(StringType())),
        (data_acxiom_poptn_upd.miACS_27_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_27_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_10_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_09_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_09_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_09_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_09_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_19_030.cast(IntegerType())),
        (data_acxiom_poptn_upd.miACS_19_001.cast(IntegerType())),
        (data_acxiom_poptn_upd.miACS_07_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_07_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_07_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_16_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_011.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_012.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_014.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_013.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_016.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_015.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_018.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_017.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_06_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_011.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_012.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_013.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_06_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_06_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_014.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_015.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_016.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_05_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_06_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_05_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_05_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_011.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_012.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_013.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_017.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_030.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_018.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_019.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_03_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_020.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_021.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_022.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_023.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_024.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_025.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_026.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_027.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_031.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_028.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_04_029.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_02_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_17_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_18_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_18_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_18_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_18_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_17_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_12_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_12_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_12_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_12_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_028.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_029.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_030.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_031.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_014.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_015.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_013.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_017.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_018.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_016.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_003.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_004.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_006.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_005.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_01_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_011.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_012.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_01_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_13_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_14_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_001.cast(DecimalType(4,2))),
        (data_acxiom_poptn_upd.miACS_15_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_15_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_005.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_23_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.IBE7964B_16.cast(IntegerType())),
        (data_acxiom_poptn_upd.IBE7964B_17.cast(StringType())),
        (data_acxiom_poptn_upd.miACS_00_001.cast(IntegerType())),
        (data_acxiom_poptn_upd.miACS_00_003.cast(IntegerType())),
        (data_acxiom_poptn_upd.miACS_00_002.cast(IntegerType())),
        (data_acxiom_poptn_upd.miACS_24_026.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_023.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_028.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_027.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_037.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_022.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_020.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_029.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_035.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_018.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_024.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_032.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_034.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_030.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_019.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_021.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_025.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_033.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_036.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_031.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_038.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_015.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_001.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_014.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_003.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_012.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_010.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_013.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_009.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_004.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_002.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_016.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_011.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_017.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_006.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_007.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_008.cast(DecimalType(3,2))),
        (data_acxiom_poptn_upd.miACS_24_005.cast(DecimalType(3,2)))

    )
    
    return data_acxiom_poptn_new

# Update column names as per data dictionary
def updateColumnNamesInPoptnTable(data_acxiom_poptn):
    
    Data_list = ["ACXIOM_POPTN_DTL_KEY", 
                 "CRET_TS", 
                 "CHNL_CD", 
                 "CHNL_SRC_CD", 
                 "ADDR_LNK_KEY", 
                 "HSHLD_LNK_KEY",
                 "CURR_RCD_IND",
                 "POPTN_DNSTY_CD",
                 "POPTN_DNSTY_PRCSN_LVL_CD",
                 "CIVIL_NON_INSTTN_POPTN_PRVT_INS_PURCHS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_EMPLR_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_MEDCD_PCT",
                 "CIVIL_NON_INSTTN_POPTN_MEDCR_PCT",
                 "CIVIL_NON_INSTTN_POPTN_NO_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_PRVT_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_PUB_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_TRI_CARE_INS_PCT",
                 "CIVIL_NON_INSTTN_POPTN_VET_INS_PCT",
                 "POPTN_AGE_30_PLUS_GRNDPRNT_RESPBL_OWN_CHLDRN_PCT",
                 "TOTL_POPTN_FAM_HSHLD_PCT",
                 "TOTL_POPTN_GRP_QTR_PCT",
                 "TOTL_POPTN_IN_HSHLD_PCT",
                 "TOTL_POPTN_NOT_IN_FAM_HSHLD_PCT",
                 "POPTN_INCM_HISP_ORIG_AMT",
                 "TOTL_POPTN_PER_CAPITA_INCM_AMT",
                 "POPTN_AGE_1_PLUS_LIVE_IN_DIFF_QTR_PCT",
                 "POPTN_AGE_1_PLUS_LIVE_IN_DIFF_ST_OR_CNTRY_PCT",
                 "POPTN_AGE_1_PLUS_LIVE_SAME_QTR_PCT",
                 "POPTN_AGE_5_PLUS_ASIAN_LANG_PCT",
                 "POPTN_AGE_5_PLUS_ENG_LANG_ONLY_PCT",
                 "POPTN_AGE_5_PLUS_ENG_SPKN_NOT_SKILLED_PCT",
                 "POPTN_AGE_5_PLUS_ENG_SPKN_SKILLED_PCT",
                 "POPTN_AGE_5_PLUS_OTHR_LANG_PCT",
                 "POPTN_AGE_5_PLUS_OTHR_IND_EURP_LANG_PCT",
                 "POPTN_AGE_5_PLUS_SPNSH_LANG_PCT",
                 "POPTN_ASIAN_ALONE_ASIAN_IND_PCT",
                 "POPTN_ASIAN_ALONE_CHN_PCT",
                 "POPTN_ASIAN_ALONE_FLP_PCT",
                 "POPTN_ASIAN_ALONE_JPN_PCT",
                 "POPTN_ASIAN_ALONE_KOR_PCT",
                 "POPTN_ASIAN_ALONE_OTHR_ASIAN_PCT",
                 "POPTN_ASIAN_ALONE_VIET_PCT",
                 "POPTN_NTV_HAWAI_ALONE_GUA_OR_CHAM_PCT",
                 "POPTN_NTV_HAWAI_ALONE_PCT",
                 "POPTN_NTV_HAWAI_ALONE_OTHR_PAC_ISL_PCT",
                 "POPTN_NTV_HAWAI_ALONE_SAM_PCT",
                 "TOTL_POPTN_AMER_PCT",
                 "TOTL_POPTN_AIAN_ALONE_PCT",
                 "TOTL_POPTN_ARAB_PCT",
                 "TOTL_POPTN_ASIAN_ALONE_PCT",
                 "TOTL_POPTN_BLACK_ALONE_PCT",
                 "TOTL_POPTN_BRTSH_PCT",
                 "TOTL_POPTN_CUBAN_PCT",
                 "TOTL_POPTN_CZECH_PCT",
                 "TOTL_POPTN_DAN_PCT",
                 "TOTL_POPTN_DUTCH_PCT",
                 "TOTL_POPTN_ENG_PCT",
                 "TOTL_POPTN_EURP_PCT",
                 "TOTL_POPTN_FORGN_BORN_PCT",
                 "TOTL_POPTN_FRNCH_PCT",
                 "TOTL_POPTN_FRNCH_CAND_PCT",
                 "TOTL_POPTN_GRMN_PCT",
                 "TOTL_POPTN_GREEK_PCT",
                 "TOTL_POPTN_HISP_PCT",
                 "TOTL_POPTN_HUNG_PCT",
                 "TOTL_POPTN_IN_OTHR_ST_IN_US_PCT",
                 "TOTL_POPTN_IN_RSDNC_ST_PCT",
                 "TOTL_POPTN_IRISH_PCT",
                 "TOTL_POPTN_ITL_PCT",
                 "TOTL_POPTN_LITH_PCT",
                 "TOTL_POPTN_MEX_PCT",
                 "TOTL_POPTN_NTV_CITZ_PCT",
                 "TOTL_POPTN_NTV_BORN_OUTSD_US_PCT",
                 "TOTL_POPTN_NTV_HAWAI_ALONE_PCT",
                 "TOTL_POPTN_NATLZD_CITZ_PCT",
                 "TOTL_POPTN_NON_CITZ_PCT",
                 "TOTL_POPTN_NON_HISP_ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_AIAN__ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_ASIAN_ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_BLACK_ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_NTV_HAWAI_ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_OTHR_RACE_ALONE_PCT",
                 "TOTL_POPTN_NON_HISP_TWO_OR_MORE_RACE_PCT",
                 "TOTL_POPTN_NON_HISP_WHITE_ALONE_PCT",
                 "TOTL_POPTN_NORW_PCT",
                 "TOTL_POPTN_OTHR_PCT",
                 "TOTL_POPTN_OTHR_HISP_OR_LTN_ORIG_PCT",
                 "TOTL_POPTN_OTHR_RACE_ALONE_PCT",
                 "TOTL_POPTN_POL_PCT",
                 "TOTL_POPTN_PRTG_PCT",
                 "TOTL_POPTN_PURCN_PCT",
                 "TOTL_POPTN_RUSS_PCT",
                 "TOTL_POPTN_SCTIRS_PCT",
                 "TOTL_POPTN_SCTT_PCT",
                 "TOTL_POPTN_SLV_PCT",
                 "TOTL_POPTN_SSA_PCT",
                 "TOTL_POPTN_SWD_PCT",
                 "TOTL_POPTN_SWISS_PCT",
                 "TOTL_POPTN_TWO_OR_MORE_PCT",
                 "TOTL_POPTN_UKR_PCT",
                 "TOTL_POPTN_UNCLSF_OR_NR_PCT",
                 "TOTL_POPTN_WELCH_PCT",
                 "TOTL_POPTN_WEST_INDN_PCT",
                 "TOTL_POPTN_WHITE_ALONE_PCT",
                 "POPTN_LT_AGE_18_UND_PVRTY_LVL_PCT",
                 "POPTN_NON_INSTTN_WTH_DISBLTY_PCT",
                 "POPTN_NON_INSTTN_AGE_BTWN_18_AND_64_WTH_DISBLTY_PCT",
                 "POPTN_NON_INSTTN_AGE_65_PLUS_WTH_DISBLTY_PCT",
                 "POPTN_NON_INSTTN_AGE_LT_18_WTH_DISBLTY_PCT",
                 "TOTL_POPTN_UND_PVRTY_LVL_PCT",
                 "POPTN_AGE_15_PLUS_MRD_PCT",
                 "POPTN_AGE_15_PLUS_NVR_MRD_PCT",
                 "POPTN_AGE_15_PLUS_DIVO_PCT",
                 "POPTN_AGE_15_PLUS_WID_PCT",
                 "POPTN_AGE_18_PLUS_AVG_AGE_NUM",
                 "POPTN_AGE_18_PLUS_MEDN_AGE_NUM",
                 "POPTN_AGE_25_PLUS_AVG_AGE_NUM",
                 "POPTN_AGE_25_PLUS_MEDN_AGE_NUM",
                 "POPTN_AGE_BTWN_18_AND_64_BLACK_ALONE_PCT",
                 "POPTN_AGE_65_PLUS_BLACK_ALONE_PCT",
                 "POPTN_AGE_LT_18_BLACK_ALONE_PCT",
                 "POPTN_AGE_BTWN_18_AND_64_HISP_PCT",
                 "POPTN_AGE_65_PLUS_HISP_PCT",
                 "POPTN_AGE_LT_18_HISP_PCT",
                 "TOTL_POPTN_AVG_AGE_NUM",
                 "TOTL_POPTN_MEDN_AGE_NUM",
                 "FEM_POPTN_MEDN_AGE_NUM",
                 "MALE_POPTN_MEDN_AGE_NUM",
                 "TOTL_POPTN_AGE_BTWN_18_AND_64_PCT",
                 "TOTL_POPTN_AGE_65_PLUS_PCT",
                 "TOTL_POPTN_FEM_PCT",
                 "TOTL_POPTN_MALE_PCT",
                 "TOTL_POPTN_AGE_LT_18_PCT",
                 "POPTN_AGE_BTWN_18_AND_64_WHT_PCT",
                 "POPTN_AGE_65_PLUS_WHITE_PCT",
                 "POPTN_AGE_LT_18_WHITE_PCT",
                 "POPTN_AGE_BTWN_15_AND_50_GAVE_BRTH_IN_PAST_12_MTHS_PCT",
                 "POPTN_AGE_3_PLUS_ENRL_IN_COLLEGE_PCT",
                 "POPTN_AGE_3_PLUS_ENRL_IN_ELMNT_OR_HIGH_SCHL_PCT",
                 "POPTN_AGE_3_PLUS_ENRL_IN_PRE_SCHL_PCT",
                 "POPTN_AGE_3_PLUS_ENRL_IN_PRVT_SCHL_PCT",
                 "POPTN_AGE_3_PLUS_ENRL_IN_PUB_SCHL_PCT",
                 "POPTN_AGE_3_PLUS_NOT_ENRL_PCT",
                 "POPTN_AGE_25_PLUS_MEDN_EDUCNL_ATTNMT_NUM",
                 "POPTN_AGE_25_PLUS_ASSOC_DGRE_PCT",
                 "POPTN_AGE_25_PLUS_BA_DGRE_PCT",
                 "POPTN_AGE_25_PLUS_CMPLT_HIGH_SCHL_PCT",
                 "POPTN_AGE_25_PLUS_CMPLT_GRADE_0_8_PCT",
                 "POPTN_AGE_25_PLUS_GRAD_DGRE_PCT",
                 "POPTN_AGE_25_PLUS_ATTNDED_SOME_COLLEGE_BUT_NO_DGRE_PCT",
                 "POPTN_AGE_25_PLUS_ATTNDED_SOME_HIGH_SCHL_PCT",
                 "POPTN_AGE_16_PLUS_FEM_ACTV_MLTRY_DUTY_PCT",
                 "POPTN_AGE_16_PLUS_MALE_ACTV_MLTRY_DUTY_PCT",
                 "POPTN_AGE_16_PLUS_ACTV_MLTRY_DUTY_PCT",
                 "POPTN_AGE_16_PLUS_FEM_EMPLY_PCT",
                 "POPTN_AGE_16_PLUS_MALE_EMPLY_PCT",
                 "POPTN_AGE_16_PLUS_EMPLY_PCT",
                 "POPTN_AGE_16_PLUS_FEM_IN_WRKNG_FRC_PCT",
                 "POPTN_AGE_16_PLUS_MALE_IN_WRKNG_FRC_PCT",
                 "POPTN_AGE_16_PLUS_IN_WRKNG_FRC_PCT",
                 "POPTN_AGE_BTWN_20_AND_64_WTH_OWN_CHLDRN_EMPLY_PCT",
                 "GEO_PLC_CD",
                 "GEO_PLC_NM",
                 "TOTL_POPTN_PPL_CNT",
                 "GRP_QTR_POPTN_PPL_CNT",
                 "HSHLD_POPTN_PPL_CNT",
                 "EMPLY_CVL_POPTN_16_PLUS_OFFC_ADMIN_SUPRT_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_BLDG_GRND_CLEAN_AND_MAINT_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_CONSTRN_EXTRTN_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_FARMG_FSHNG_FOR_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_FED_GOVT_EMP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_FOOD_PREP_SVC_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_HC_SUPRT_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_INSTLTN_MAINT_REPAIR_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_LOCAL_GOVT_EMP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_MGT_BUS_FIN_OPS_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PERSNL_CARE_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PRVT_CO_EMP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PRVT_NON_PRFT_WAGE_OR_SAL_WRKR_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PROD_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PROFL_RELT_OCCUP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_PROT_SVC_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_SALES_RELT_OCCUP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_SELF_EMPLY_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_ST_GOVT_EMP_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_TRNSPRTN_MATRLS_MOVE_PCT",
                 "EMPLY_CVL_POPTN_16_PLUS_UNPD_FAM_WRKR_PCT",
                 "EMPLY_POPTN_16_PLUS_ACCOMTN_FOOD_SVCS_PCT",
                 "EMPLY_POPTN_16_PLUS_AGRI_PCT",
                 "EMPLY_POPTN_16_PLUS_ARTS_ENTMT_RCRTON_PCT",
                 "EMPLY_POPTN_16_PLUS_CONSTRN_PCT",
                 "EMPLY_POPTN_16_PLUS_EDUCNL_SVCS_PCT",
                 "EMPLY_POPTN_16_PLUS_FINANC_INS_REAL_EST_PCT",
                 "EMPLY_POPTN_16_PLUS_HLTH_SOCIAL_ASSTNC_PCT",
                 "EMPLY_POPTN_16_PLUS_INFO_COMMTN_PCT",
                 "EMPLY_POPTN_16_PLUS_MFRG_PCT",
                 "EMPLY_POPTN_16_PLUS_MINING_PCT",
                 "EMPLY_POPTN_16_PLUS_OTHR_SVCS_PCT",
                 "EMPLY_POPTN_16_PLUS_PROFL_SCI_MGT_ADMINV_WASTE_MGMNT_SVCS_PCT",
                 "EMPLY_POPTN_16_PLUS_PUB_ADMIN_PCT",
                 "EMPLY_POPTN_16_PLUS_RETL_PCT",
                 "EMPLY_POPTN_16_PLUS_TRNSPRTN_OR_WRHSE_PCT",
                 "EMPLY_POPTN_16_PLUS_UTIL_PCT",
                 "EMPLY_POPTN_16_PLUS_WHSL_TRADE_PCT"]
        
    data_acxiom_poptn_final = data_acxiom_poptn.toDF(*Data_list)
    #data_acxiom_age_final.show()
                                      
    return data_acxiom_poptn_final


# Add standard columns to the table
def addDefaultColumns(dataAcxiomChangeDataTypeDf):
    acxiomHouseHoldHeaderDf = (
        dataAcxiomChangeDataTypeDf.withColumn(
            "ACXIOM_HSHLD_PURCHS_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return acxiomHouseHoldHeaderDf

# Prepare house hold purchase table
def prepareHouseHoldpurchsChangeDataType(dataAcxiomChangeDataTypeDf):  

    houseHoldTempDf = addDefaultColumns(dataAcxiomChangeDataTypeDf)
    data_acxiom_hhpurchs_hkc = addhousekeepingColumns(data_acxiom)
    
    dataAcxiomChangeDataTypeDf = houseHoldTempDf.select(
        (houseHoldTempDf.ACXIOM_HSHLD_PURCHS_DTL_KEY.cast(StringType())),
        (data_acxiom_hhpurchs_hkc.CRET_TS.cast(TimestampType())),
        (data_acxiom_hhpurchs_hkc.CHNL_CD.cast(StringType())),
        (data_acxiom_hhpurchs_hkc.CHNL_SRC_CD.cast(StringType())),
        (data_acxiom_hhpurchs_hkc.ADDR_LNK_KEY.cast(StringType())),
        (data_acxiom_hhpurchs_hkc.HSHLD_LNK_KEY.cast(StringType())),
        (data_acxiom_hhpurchs_hkc.CURR_RCD_IND.cast(StringType())),
        (houseHoldTempDf.IBE6136.cast(ByteType())),
        (houseHoldTempDf.IBE2022.cast(ByteType())),
        (houseHoldTempDf.IBE6142.cast(ByteType())),
        (houseHoldTempDf.IBE6254.cast(ByteType())),
        (houseHoldTempDf.IBE6257.cast(ByteType())),
        (houseHoldTempDf.IBE6764.cast(ByteType())),
        (houseHoldTempDf.IBE6412.cast(ByteType())),
        (houseHoldTempDf.IBE6414.cast(ByteType())),
        (houseHoldTempDf.IBE6426.cast(ByteType())),
        (houseHoldTempDf.IBE6427.cast(ByteType())),
        (houseHoldTempDf.IBE6428.cast(ByteType())),
        (houseHoldTempDf.IBE6429.cast(ByteType())),
        (houseHoldTempDf.IBE6430.cast(ByteType())),
        (houseHoldTempDf.IBE6431.cast(ByteType())),
        (houseHoldTempDf.IBE6432.cast(ByteType())),
        (houseHoldTempDf.IBE6433.cast(ByteType())),
        (houseHoldTempDf.IBE6504.cast(ByteType())),
        (houseHoldTempDf.IBE6802.cast(ByteType())),
        (houseHoldTempDf.IBE6539.cast(ByteType())),
        (houseHoldTempDf.IBE6540.cast(ByteType())),
        (houseHoldTempDf.IBE6794.cast(ByteType())),
        (houseHoldTempDf.IBE6819.cast(ByteType())),
        (houseHoldTempDf.IBE6143.cast(ByteType())),
        (houseHoldTempDf.IBE2024.cast(ByteType())),
        (houseHoldTempDf.IBE6456.cast(ByteType())),
        (houseHoldTempDf.IBE2029.cast(ByteType())),
        (houseHoldTempDf.IBE6398.cast(ByteType())),
        (houseHoldTempDf.IBE2030.cast(ByteType())),
        (houseHoldTempDf.IBE6541.cast(ByteType())),
        (houseHoldTempDf.IBE6793.cast(ByteType())),
        (houseHoldTempDf.IBE6523.cast(ByteType())),
        (houseHoldTempDf.IBE6526.cast(ByteType())),
        (houseHoldTempDf.IBE6881.cast(ByteType())),
        (houseHoldTempDf.IBE6249.cast(ByteType())),
        (houseHoldTempDf.IBE6792.cast(ByteType())),
        (houseHoldTempDf.IBE6236.cast(ByteType())),
        (houseHoldTempDf.IBE6588.cast(ByteType())),
        (houseHoldTempDf.IBE6590.cast(ByteType())),
        (houseHoldTempDf.IBE6597.cast(ByteType())),
        (houseHoldTempDf.IBE6598.cast(ByteType())),
        (houseHoldTempDf.IBE6182.cast(ByteType())),
        (houseHoldTempDf.IBE6184.cast(ByteType())),
        (houseHoldTempDf.IBE6189.cast(ByteType())),
        (houseHoldTempDf.IBE6190.cast(ByteType())),
        (houseHoldTempDf.IBE6884.cast(ByteType())),
        (houseHoldTempDf.IBE6885.cast(ByteType())),
        (houseHoldTempDf.IBE6892.cast(ByteType())),
        (houseHoldTempDf.IBE6893.cast(ByteType())),
        (houseHoldTempDf.IBE6894.cast(ByteType())),
        (houseHoldTempDf.IBE6898.cast(ByteType())),
        (houseHoldTempDf.IBE2000.cast(ByteType())),
        (houseHoldTempDf.IBE2003.cast(ByteType())),
        (houseHoldTempDf.IBE2005.cast(ByteType())),
        (houseHoldTempDf.IBE2007.cast(ByteType())),
        (houseHoldTempDf.IBE2009.cast(ByteType())),
        (houseHoldTempDf.IBE2010.cast(ByteType())),
        (houseHoldTempDf.IBE2012.cast(ByteType())),
        (houseHoldTempDf.IBE2013.cast(ByteType())),
        (houseHoldTempDf.IBE2014.cast(ByteType())),
        (houseHoldTempDf.IBE2015.cast(ByteType())),
        (houseHoldTempDf.IBE2016.cast(ByteType())),
        (houseHoldTempDf.IBE6219.cast(ByteType())),
        (houseHoldTempDf.IBE6222.cast(ByteType())),
        (houseHoldTempDf.IBE6224.cast(ByteType())),
        (houseHoldTempDf.IBE6229.cast(ByteType())),
        (houseHoldTempDf.IBE6233.cast(ByteType())),
        (houseHoldTempDf.IBE6235.cast(ByteType())),
        (houseHoldTempDf.IBE6207.cast(ByteType())),
        (houseHoldTempDf.IBE6558.cast(ByteType())),
        (houseHoldTempDf.IBE6565.cast(ByteType())),
        (houseHoldTempDf.IBE6566.cast(ByteType())),
        (houseHoldTempDf.IBE6568.cast(ByteType())),
        (houseHoldTempDf.IBE6570.cast(ByteType())),
        (houseHoldTempDf.IBE6573.cast(ByteType())),
        (houseHoldTempDf.IBE6574.cast(ByteType())),
        (houseHoldTempDf.IBE6575.cast(ByteType())),
        (houseHoldTempDf.IBE6576.cast(ByteType())),
        (houseHoldTempDf.IBE6577.cast(ByteType())),
        (houseHoldTempDf.IBE6312.cast(ByteType())),
        (houseHoldTempDf.IBE6586.cast(ByteType())),
        (houseHoldTempDf.IBE6763.cast(ByteType())),
        (houseHoldTempDf.IBE6826.cast(ByteType())),
        (houseHoldTempDf.IBE6166.cast(ByteType())),
        (houseHoldTempDf.IBE6167.cast(ByteType())),
        (houseHoldTempDf.IBE6168.cast(ByteType())),
        (houseHoldTempDf.IBE6252.cast(ByteType())),
        (houseHoldTempDf.IBE6760.cast(ByteType())),
        (houseHoldTempDf.IBE6277.cast(ByteType())),
        (houseHoldTempDf.IBE6294.cast(ByteType())),
        (houseHoldTempDf.IBE6341.cast(ByteType())),
        (houseHoldTempDf.IBE6262.cast(ByteType())),
        (houseHoldTempDf.IBE6262.cast(ByteType())),
        (houseHoldTempDf.IBE6362.cast(ByteType())),
        (houseHoldTempDf.IBE6365.cast(ByteType())),
        (houseHoldTempDf.IBE6366.cast(ByteType())),
        (houseHoldTempDf.IBE6584.cast(ByteType())),
        (houseHoldTempDf.IBE6263.cast(ByteType())),
        (houseHoldTempDf.IBE6361.cast(ByteType())),
        (houseHoldTempDf.IBE6416.cast(ByteType())),
        (houseHoldTempDf.IBE6179.cast(ByteType())),
        (houseHoldTempDf.IBE6180.cast(ByteType())),
        (houseHoldTempDf.IBE6181.cast(ByteType())),
        (houseHoldTempDf.IBE6359.cast(ByteType())),
        (houseHoldTempDf.IBE6349.cast(ByteType())),
        (houseHoldTempDf.IBE6434.cast(ByteType())),
        (houseHoldTempDf.IBE6435.cast(ByteType())),
        (houseHoldTempDf.IBE6436.cast(ByteType())),
        (houseHoldTempDf.IBE6437.cast(ByteType())),
        (houseHoldTempDf.IBE6438.cast(ByteType())),
        (houseHoldTempDf.IBE6439.cast(ByteType())),
        (houseHoldTempDf.IBE6449.cast(ByteType())),
        (houseHoldTempDf.IBE6450.cast(ByteType())),
        (houseHoldTempDf.IBE6452.cast(ByteType())),
        (houseHoldTempDf.IBE6453.cast(ByteType())),
        (houseHoldTempDf.IBE6425.cast(ByteType())),
        (houseHoldTempDf.IBE6441.cast(ByteType())),
        (houseHoldTempDf.IBE6442.cast(ByteType())),
        (houseHoldTempDf.IBE6443.cast(ByteType())),
        (houseHoldTempDf.IBE6444.cast(ByteType())),
        (houseHoldTempDf.IBE6440.cast(ByteType())),
        (houseHoldTempDf.IBE6445.cast(ByteType())),
        (houseHoldTempDf.IBE6446.cast(ByteType())),
        (houseHoldTempDf.IBE6447.cast(ByteType())),
        (houseHoldTempDf.IBE6448.cast(ByteType())),
        (houseHoldTempDf.IBE6585.cast(ByteType())),
        (houseHoldTempDf.IBE6734.cast(ByteType())),
        (houseHoldTempDf.IBE6735.cast(ByteType())),
        (houseHoldTempDf.IBE6736.cast(ByteType())),
        (houseHoldTempDf.IBE6737.cast(ByteType())),
        (houseHoldTempDf.IBE6738.cast(ByteType())),
        (houseHoldTempDf.IBE6762.cast(ByteType())),
        (houseHoldTempDf.IBE6505.cast(ByteType())),
        (houseHoldTempDf.IBE6503.cast(ByteType())),
        (houseHoldTempDf.IBE6515.cast(ByteType())),
        (houseHoldTempDf.IBE6516.cast(ByteType())),
        (houseHoldTempDf.IBE6549.cast(ByteType())),
        (houseHoldTempDf.IBE6388.cast(ByteType())),
        (houseHoldTempDf.IBE6389.cast(ByteType())),
        (houseHoldTempDf.IBE6392.cast(ByteType())),
        (houseHoldTempDf.IBE6391.cast(ByteType())),
        (houseHoldTempDf.IBE6801.cast(ByteType())),
        (houseHoldTempDf.IBE6459.cast(ByteType())),
        (houseHoldTempDf.IBE6833.cast(ByteType())),
        (houseHoldTempDf.IBE6834.cast(ByteType())),
        (houseHoldTempDf.IBE6836.cast(ByteType())),
        (houseHoldTempDf.IBE6837.cast(ByteType())),
        (houseHoldTempDf.IBE8615.cast(StringType())),
        (houseHoldTempDf.IBE8167_01.cast(ByteType())),
        (houseHoldTempDf.IBE8167_02.cast(ByteType())),
        (houseHoldTempDf.IBE8167_03.cast(ByteType())),
        (houseHoldTempDf.IBE8167_04.cast(ByteType())),
        (houseHoldTempDf.IBE8167_05.cast(ByteType())),
        (houseHoldTempDf.IBE8167_06.cast(ByteType())),
        (houseHoldTempDf.IBE8167_07.cast(ByteType())),
        (houseHoldTempDf.IBE8167_08.cast(ByteType())),
        (houseHoldTempDf.IBE8167_09.cast(ByteType())),
        (houseHoldTempDf.IBE8167_10.cast(ByteType())),
        (houseHoldTempDf.IBE8167_11.cast(ByteType())),
        (houseHoldTempDf.IBE8167_12.cast(ByteType())),
        (houseHoldTempDf.IBE8167_13.cast(ByteType())),
        (houseHoldTempDf.IBE8167_14.cast(ByteType())),
        (houseHoldTempDf.IBE8167_15.cast(ByteType())),
        (houseHoldTempDf.IBE8167_16.cast(ByteType())),
        (houseHoldTempDf.IBE8167_17.cast(ByteType())),
        (houseHoldTempDf.IBE8167_18.cast(ByteType())),
        (houseHoldTempDf.IBE8167_19.cast(ByteType())),
        (houseHoldTempDf.IBE8167_20.cast(ByteType())),
        (houseHoldTempDf.IBE8167_21.cast(ByteType())),
        (houseHoldTempDf.IBE8167_22.cast(ByteType())),
        (houseHoldTempDf.IBE8167_23.cast(ByteType())),
        (houseHoldTempDf.IBE8167_24.cast(ByteType())),
        (houseHoldTempDf.IBE8167_25.cast(ByteType())),
        (houseHoldTempDf.IBE8167_26.cast(ByteType())),
        (houseHoldTempDf.IBE8167_27.cast(ByteType())),
        (houseHoldTempDf.IBE8167_28.cast(ByteType())),
        (houseHoldTempDf.IBE8167_29.cast(ByteType())),
        (houseHoldTempDf.IBE8167_30.cast(ByteType())),
        (houseHoldTempDf.IBE8167_31.cast(ByteType())),
        (houseHoldTempDf.IBE8177.cast(StringType())),
        (houseHoldTempDf.IBE8620.cast(StringType()))

    )
    
    return dataAcxiomChangeDataTypeDf

# Update column names as per data dictionary
def updateColumnNamesInHouseHoldpurchsTable(dataAcxiomChangeDataTypeDf):    
    Data_list = ["ACXIOM_HSHLD_PURCHS_DTL_KEY", 
    "CRET_TS", 
    "CHNL_CD", 
    "CHNL_SRC_CD", 
    "ADDR_LNK_KEY", 
    "HSHLD_LNK_KEY",
    "CURR_RCD_IND",
    "CTGRY_MEN_APRL_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_WMEN_APRL_PURCHS_IN_LAST_24_MTHS_IND",
    "CTGRY_WMEN_APRL_PLUS_SIZE_PURCHS_IN_LAST_24_MTHS_IND",
    "CTGRY_CHLD_CARE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "CTGRY_CHLD_CARE_PRODT_TOY_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_SHIP_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLDY_GIFT_ITEMS_PARTY_GOODS_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLDY_GIFT_ITEMS_STATNY_GOODS_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_ACCRS_PURCHS_IN_LAST_24_MTHS_IND",
    "CTGRY_HLTH_AND_BUTY_CSMTC_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_FEM_WELLNESS_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_MED_SUPLY_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_NEW_AGE_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_VITMN_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_PERSNL_CARE_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HLTH_AND_BUTY_PHY_ENHNCMT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_HOME_FRNSNG_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_SWMNG_POOL_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_LIFSTYL_INT_AND_PASN_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_LIFSTYL_INT_AND_PASN_NVLTY_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_SPRT_AND_LSRE_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "CTGRY_TRVL_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_GENL_APRL_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_ART_AND_ANTQ_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_CHLDRNS_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_FOOD_AND_BVRG_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_HLDY_GIFT_ITEMS_PURCHS_IN_LAST_24_MTHS_IND",
    "SUPER_CTGRY_HLTH_AND_BUTY_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_LIFSTYL_INT_AND_PASN_PURCHS_IN_LAST_24_MTHS_IND", 
    "SUPER_CTGRY_SPRT_AND_LSRE_PURCHS_IN_LAST_24_MTHS_IND", 
    "CSTM_JWLRY_PURCHS_IN_LAST_24_MTHS_IND",
    "HNCFT_JWLRY_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PURCHS_IN_LAST_24_MTHS_IND", 
    "CHLDRNS_APRL_PURCHS_IN_LAST_24_MTHS_IND", 
    "SPRT_AND_LSRE_APRL_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOYS_APRL_PRE_TEEN_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_GENL_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_ACTVW_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_OUTWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_SWMWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_BIG_AND_TALL_GENL_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_BIG_AND_TALL_ACTVW_PURCHS_IN_LAST_24_MTHS_IND",
    "MEN_APRL_BIG_AND_TALL_OUTWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "MEN_APRL_BIG_AND_TALL_SWMWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_BUS_CSUL_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_CSUL_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_MATRN_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_OUTWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_SWMWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PTTE_SIZE_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PTTE_SIZE_CSUL_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PTTE_SIZE_OUTWR_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_ACTVW_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_CSUL_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_EVNWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_SLPWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_MATRN_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_OUTWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_SWMWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "WMEN_APRL_PLUS_SIZE_UNDWR_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_COKNG_FOOD_WINE_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_HLTH_MIND_BODY_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_HOW_TO_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_PRNT_AND_FAM_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_SPRT_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_TRVL_PURCHS_IN_LAST_24_MTHS_IND", 
    "BOOKS_AUDIO_HLTH_MIND_AND_BODY_PURCHS_IN_LAST_24_MTHS",
    "MAGAZE_BABY_PRNT_PURCHS_IN_LAST_24_MTHS_IND",  
    "MAGAZE_FITNS_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_FOOD_COKNG_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_HLTH_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_MEN_INT_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_OUTDR_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_SCI_TCHNLGY_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_SPRTRCRTON_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_TRVL_LSRE_PURCHS_IN_LAST_24_MTHS_IND", 
    "MAGAZE_WMEN_INT_PURCHS_IN_LAST_24_MTHS_IND",
    "DNTN_CONTRIBTN_IN_LAST_24_MTHS_IND", 
    "MBRSHP_CLUB_PURCHS_IN_LAST_24_MTHS_IND", 
    "SHIP_PRODT_PURCHS_IN_LAST_24_MTHS_IND", 
    "VAL_PRC_GENL_MERCHD_PURCHS_IN_LAST_24_MTHS_IND", 
    "BABY_CARE_PURCHS_IN_LAST_24_MTHS_IND", 
    "BABY_HOME_DECOR_PURCHS_IN_LAST_24_MTHS_IND", 
    "BABY_TOYS_PURCHS_IN_LAST_24_MTHS_IND", 
    "CHLDRN_LEARN_AND_ACTY_TOY_PURCHS_IN_LAST_24_MTHS_IND", 
    "SCI_AND_NTRE_TOY_PURCHS_IN_LAST_24_MTHS_IND",
    "SPRT_COLTBLE_PURCHS_IN_LAST_24_MTHS_IND", 
    "GENL_CRAFT_HOBBY_PURCHS_IN_LAST_24_MTHS_IND", 
    "ELECTRNC_MSSANG_PURCHS_IN_LAST_24_MTHS_IND",
    "CHCLT_CANDY_PURCHS_IN_LAST_24_MTHS_IND", 
    "COOKIE_BRWN_PURCHS_IN_LAST_24_MTHS_IND",
    "FOOD_BVRG_PURCHS_IN_LAST_24_MTHS_IND",
    "FRT_NUT_PURCHS_IN_LAST_24_MTHS_IND",
    "FRT_PURCHS_IN_LAST_24_MTHS_IND",
    "MBRSHP_CLUB_FOOD_PURCHS_IN_LAST_24_MTHS_IND",
    "CHRTMS_PURCHS_IN_LAST_24_MTHS_IND",
    "FLWRS_PURCHS_IN_LAST_24_MTHS_IND",
    "BUTY_AND_WELLNESS_PURCHS_IN_LAST_24_MTHS_IND",
    "BUTY_ASSRS_PURCHS_IN_LAST_24_MTHS_IND",
    "CSMTC_BUTY_AIDS_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "PHY_ENHNCMT_BUTY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "FITNS_EQUIP_PURCHS_IN_LAST_24_MTHS_IND",
    "EXCSE_HLTH_VIDEO_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "HLTH_ANTI_AGE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "CRDC_HLTH_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "DIET_WGHT_LOSS_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "FEM_WELLNESS_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "GREEN_HLTH_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "JOINT_MOBLTY_HLTH_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "NEW_AGE_HLTH_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "VITMN_HLTH_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "VITMN_NUTRTN_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "WGHT_GAIN_MSCL_BLDG_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "HLTH_AND_BUTY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "ALRGY_RELT_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "ALTRNTV_MEDCN_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "MOBLTY_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "DIABTC_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "ORTPC_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "SNR_NEED_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "SIGHT_NEED_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "SIGHT_RELT_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "SUN_PROT_MED_SUPLY_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "MBRSHP_SELF_HELP_CLUB_PURCHS_IN_LAST_24_MTHS_IND",
    "PERSNL_CARE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "INTMT_PERSNL_CARE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "MEN_PERSNL_CARE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "WMEN_PERSNL_CARE_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "PERSNL_SFTY_PRODT_SPRT_AND_LSRE_PURCHS_IN_LAST_24_MTHS_IND",
    "SELF_HELP_VIDEO_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "HOME_FRNSNG_ACCRS_PURCHS_IN_LAST_24_MTHS_IND",
    "HOME_FRNSNG_PURCHS_IN_LAST_24_MTHS_IND",
    "KTCHN_COKNG_TOOL_PURCHS_IN_LAST_24_MTHS_IND",
    "KTCHN_ACCRS_PURCHS_IN_LAST_24_MTHS_IND",
    "LNN_HOME_FRNSNG_PURCHS_IN_LAST_24_MTHS_IND",
    "ORGN_GRDN_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "SEEDS_FOR_GARDN_PURCHS_IN_LAST_24_MTHS_IND",
    "VGTBLS_FRT_HERBS_FOR_GARDN_PURCHS_IN_LAST_24_MTHS_IND",
    "VGTBLS_FRT_HERBS__SDS_FOR_GARDN_PURCHS_IN_LAST_24_MTHS_IND",
    "SWMNG_POOL_PRODT_PURCHS_IN_LAST_24_MTHS_IND",
    "LIFSTYL_INT_PASN_PURCHS_IN_LAST_24_MTHS_IND",
    "HOW_TO_VIDEO_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "KIDS_FAM_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "SPRT_VIDEO_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "TRVL_VIDEO_DVD_PURCHS_IN_LAST_24_MTHS_IND",
    "PRODT_PURCHS_VIA_MAIL_IND","FEM_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "JWLRY_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "MALE_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "FEM_APRL_PLUS_SIZE_PURCHS_VIA_MAIL_IND",
    "TEEN_FSHN_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "UNKN_TY_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "ART_AND_ANTQ_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "ART_AND_CRAFT_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "AUTO_SPPLS_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "BUTY_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "BOOKS_APRL_PRODT_PURCHS_VIA_MAIL_IND",
    "CHLDRN_MERCHD_PURCHS_VIA_MAIL_IND",
    "COTBLE_PURCHS_VIA_MAIL_IND",
    "COMPTR_SFTWR_PURCHS_VIA_MAIL_IND",
    "ELTNC_PURCHS_VIA_MAIL_IND",
    "EQSRN_PURCHS_VIA_MAIL_IND",
    "FOOD_PURCHS_VIA_MAIL_IND",
    "GENL_GIFT_AND_MERCHD_PURCHS_VIA_MAIL_IND",
    "GIFT_PURCHS_VIA_MAIL_IND",
    "HLTH_ITEM_PURCHS_VIA_MAIL_IND",
    "HOME_FRNSNG_AND_DECORG_PURCHS_VIA_MAIL_IND",
    "HIGH_TKT_MERCHD_PURCHS_VIA_MAIL_IND",
    "LOW_TKT_MERCHD_PURCHS_VIA_MAIL_IND",
    "MUSIC_ITEMS_PURCHS_VIA_MAIL_IND",
    "OUTDR_GARDN_ITEMS_PURCHS_VIA_MAIL_IND",
    "OUTDR_HUNTG_FSHNG_ITEMS_PURCHS_VIA_MAIL_IND",
    "PET_SPPLS_PURCHS_VIA_MAIL_IND",
    "GOLF_RELT_ITEMS_PURCHS_VIA_MAIL_IND",
    "SPRT_RELT_ITEMS_PURCHS_VIA_MAIL_IND",
    "TRVL_RELT_ITEMS_PURCHS_VIA_MAIL_IND",
    "VIDEO_DVD_PURCHS_VIA_MAIL_IND",
    "DNTN_VIA_MAIL_IND",
    "COLLATRL_RESPDR_VIA_MAIL_IND"]
                    
    dataAcxiomChangeDataTypeFinalDf = dataAcxiomChangeDataTypeDf.toDF(*Data_list)
        
    return dataAcxiomChangeDataTypeFinalDf


# Add standard columns to the table
def addDefaultColumns(dataAcxiomhhintChangeDataTypeDf):
    acxiomHouseHoldHeaderDf = (
        dataAcxiomhhintChangeDataTypeDf.withColumn(
            "ACXIOM_hhint_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return acxiomHouseHoldHeaderDf

# Prepare an hhintidual table
def prepareHouseHoldintChangeDataType(dataAcxiomhhintChangeDataTypeDf):  

    houseHoldTempDf = addDefaultColumns(dataAcxiomhhintChangeDataTypeDf)
    data_acxiom_hhint_hkc = addhousekeepingColumns(data_acxiom)
    
    dataAcxiomhhintChangeDataTypeDf = houseHoldTempDf.select(
                                (houseHoldTempDf.ACXIOM_hhint_DTL_KEY.cast(StringType())),
                                (data_acxiom_hhint_hkc.CRET_TS.cast(TimestampType())),
                                (data_acxiom_hhint_hkc.CHNL_CD.cast(StringType())),
                                (data_acxiom_hhint_hkc.CHNL_SRC_CD.cast(StringType())),
                                (data_acxiom_hhint_hkc.ADDR_LNK_KEY.cast(StringType())),
                                (data_acxiom_hhint_hkc.HSHLD_LNK_KEY.cast(StringType())),
                                (data_acxiom_hhint_hkc.CURR_RCD_IND.cast(StringType())),
                                (houseHoldTempDf.IBE7724.cast(ByteType())),
                                (houseHoldTempDf.IBE7809.cast(ByteType())),
                                (houseHoldTempDf.IBE7721.cast(ByteType())),
                                (houseHoldTempDf.IBE8653.cast(ByteType())),
                                (houseHoldTempDf.IBE7727.cast(ByteType())),
                                (houseHoldTempDf.IBE7728.cast(ByteType())),
                                (houseHoldTempDf.IBE7729.cast(ByteType())),
                                (houseHoldTempDf.IBE7821.cast(ByteType())),
                                (houseHoldTempDf.IBE7732.cast(ByteType())),
                                (houseHoldTempDf.IBE7723.cast(ByteType())),
                                (houseHoldTempDf.IBE7725.cast(ByteType())),
                                (houseHoldTempDf.IBE7733.cast(ByteType())),
                                (houseHoldTempDf.IBE7738.cast(ByteType())),
                                (houseHoldTempDf.IBE7734.cast(ByteType())),
                                (houseHoldTempDf.IBE7848.cast(ByteType())),
                                (houseHoldTempDf.IBE7737.cast(ByteType())),
                                (houseHoldTempDf.IBE7735.cast(ByteType())),
                                (houseHoldTempDf.IBE7736.cast(ByteType())),
                                (houseHoldTempDf.IBE7779.cast(ByteType())),
                                (houseHoldTempDf.IBE7777.cast(ByteType())),
                                (houseHoldTempDf.IBE7780.cast(ByteType())),
                                (houseHoldTempDf.IBE7788.cast(ByteType())),
                                (houseHoldTempDf.IBE7792.cast(ByteType())),
                                (houseHoldTempDf.IBE7791.cast(ByteType())),
                                (houseHoldTempDf.IBE7790.cast(ByteType())),
                                (houseHoldTempDf.IBE7789.cast(ByteType())),
                                (houseHoldTempDf.IBE7796.cast(ByteType())),
                                (houseHoldTempDf.IBE7801.cast(ByteType())),
                                (houseHoldTempDf.IBE7739.cast(ByteType())),
                                (houseHoldTempDf.IBE7740.cast(ByteType())),
                                (houseHoldTempDf.IBE7741.cast(ByteType())),
                                (houseHoldTempDf.IBE7743.cast(ByteType())),
                                (houseHoldTempDf.IBE7742.cast(ByteType())),
                                (houseHoldTempDf.IBE7849.cast(ByteType())),
                                (houseHoldTempDf.IBE7720.cast(ByteType())),
                                (houseHoldTempDf.IBE7771.cast(ByteType())),
                                (houseHoldTempDf.IBE2200.cast(ByteType())),
                                (houseHoldTempDf.IBE2201.cast(ByteType())),
                                (houseHoldTempDf.IBE2202.cast(ByteType())),
                                (houseHoldTempDf.IBE2203.cast(ByteType())),
                                (houseHoldTempDf.IBE2205.cast(ByteType())),
                                (houseHoldTempDf.IBE2206.cast(ByteType())),
                                (houseHoldTempDf.IBE2207.cast(ByteType())),
                                (houseHoldTempDf.IBE2208.cast(ByteType())),
                                (houseHoldTempDf.IBE7770.cast(ByteType())),
                                (houseHoldTempDf.IBE7772.cast(ByteType())),
                                (houseHoldTempDf.IBE7753.cast(ByteType())),
                                (houseHoldTempDf.IBE7817.cast(ByteType())),
                                (houseHoldTempDf.IBE7815.cast(ByteType())),
                                (houseHoldTempDf.IBE7816.cast(ByteType())),
                                (houseHoldTempDf.IBE7757.cast(ByteType())),
                                (houseHoldTempDf.IBE8326.cast(ByteType())),
                                (houseHoldTempDf.IBE8321.cast(ByteType())),
                                (houseHoldTempDf.IBE8277.cast(ByteType())),
                                (houseHoldTempDf.IBE2776.cast(ByteType())),
                                (houseHoldTempDf.IBE8279.cast(ByteType())),
                                (houseHoldTempDf.IBE8271.cast(ByteType())),
                                (houseHoldTempDf.IBE8322.cast(ByteType())),
                                (houseHoldTempDf.IBE8274.cast(ByteType())),
                                (houseHoldTempDf.IBE8276.cast(ByteType())),
                                (houseHoldTempDf.IBE7764.cast(ByteType())),
                                (houseHoldTempDf.IBE7768.cast(ByteType())),
                                (houseHoldTempDf.IBE7763.cast(ByteType())),
                                (houseHoldTempDf.IBE7760.cast(ByteType())),
                                (houseHoldTempDf.IBE7762.cast(ByteType())),
                                (houseHoldTempDf.IBE7761.cast(ByteType())),
                                (houseHoldTempDf.IBE7799.cast(ByteType())),
                                (houseHoldTempDf.IBE7766.cast(ByteType())),
                                (houseHoldTempDf.IBE7752.cast(ByteType())),
                                (houseHoldTempDf.IBE7750.cast(ByteType())),
                                (houseHoldTempDf.IBE7751.cast(ByteType())),
                                (houseHoldTempDf.IBE7759.cast(ByteType())),
                                (houseHoldTempDf.IBE7755.cast(ByteType())),
                                (houseHoldTempDf.IBE7805.cast(ByteType())),
                                (houseHoldTempDf.IBE7803.cast(ByteType())),
                                (houseHoldTempDf.IBE7773.cast(ByteType())),
                                (houseHoldTempDf.IBE7774.cast(ByteType())),
                                (houseHoldTempDf.IBE7775.cast(ByteType())),
                                (houseHoldTempDf.IBE7754.cast(ByteType())),
                                (houseHoldTempDf.IBE7758.cast(ByteType())),
                                (houseHoldTempDf.IBE7781.cast(ByteType())),
                                (houseHoldTempDf.IBE7783.cast(ByteType())),
                                (houseHoldTempDf.IBE7784.cast(ByteType())),
                                (houseHoldTempDf.IBE7782.cast(ByteType())),
                                (houseHoldTempDf.IBE7785.cast(ByteType())),
                                (houseHoldTempDf.IBE7786.cast(ByteType())),
                                (houseHoldTempDf.IBE7787.cast(ByteType())),
                                (houseHoldTempDf.IBE7808.cast(ByteType())),
                                (houseHoldTempDf.IBE7814.cast(ByteType())),
                                (houseHoldTempDf.IBE7802.cast(ByteType())),
                                (houseHoldTempDf.IBE7811.cast(ByteType())),
                                (houseHoldTempDf.IBE7804.cast(ByteType())),
                                (houseHoldTempDf.IBE7807.cast(ByteType())),
                                (houseHoldTempDf.IBE7812.cast(ByteType())),
                                (houseHoldTempDf.IBE7810.cast(ByteType())),
                                (houseHoldTempDf.IBE7744.cast(ByteType())),
                                (houseHoldTempDf.IBE7746.cast(ByteType())),
                                (houseHoldTempDf.IBE7748.cast(ByteType())),
                                (houseHoldTempDf.IBE7747.cast(ByteType())),
                                (houseHoldTempDf.IBE2529.cast(ByteType())),
                                (houseHoldTempDf.IBE2523_01.cast(ByteType())),
                                (houseHoldTempDf.IBE2523_02.cast(ByteType())),
                                (houseHoldTempDf.IBE2531_01.cast(ByteType())),
                                (houseHoldTempDf.IBE2531_02.cast(ByteType())),
                                (houseHoldTempDf.IBE7756.cast(ByteType())),
                                (houseHoldTempDf.IBE7813.cast(ByteType())),
                                (houseHoldTempDf.IBE7793.cast(ByteType())),
                                (houseHoldTempDf.IBE7794.cast(ByteType())),
                                (houseHoldTempDf.IBE7795.cast(ByteType()))
    )
    
    return dataAcxiomhhintChangeDataTypeDf


# Update column names as per data dictionary
def updateColumnNamesInHouseHoldintTable(dataAcxiomhhintChangeDataTypeDf):    
    Data_list = ["ACXIOM_HSHLD_INT_DTL_KEY", "CRET_TS", "CHNL_CD", "CHNL_SRC_CD", "ADDR_LNK_KEY", "HSHLD_LNK_KEY", "CURR_RCD_IND", "HSHLD_INT_CURR_AFFR_OR_PLTCL_IND", "HSHLD_INT_ENVRMTL_ISS_IND", 
                    "HSHLD_INT_HIST_OR_DOD_IND", "HSHLD_INT_OL_PURCHSG_IND", "HSHLD_INT_RELIG_OR_INSGHT_IND", "HSHLD_INT_SCI_IND", "HSHLD_INT_ATPCL_IND", "HSHLD_INT_CNTST_IND", 
                    "HSHLD_INT_SKILL_IND", "HSHLD_INT_SPECL_PERSN_IND", "HSHLD_INT_PRFRM_SKILLED_IND", "HSHLD_INT_READABLE_GENL_IND", "HSHLD_INT_LSTNING_AUDIO_BOOKS_IND", 
                    "HSHLD_INT_READABLE_BEST_SUPLR_IND", "HSHLD_INT_READABLE_FINANCL_JRNL_SUBSCRBR_IND", "HSHLD_INT_READABLE_JRNL_IND", "HSHLD_INT_READABLE_RELIG_IND", 
                    "HSHLD_INT_READABLE_SCI_BOOK_IND", "HSHLD_INT_CHLDRN_IND", "HSHLD_INT_PRNT_IND", "HSHLD_INT_GRNDCHLDRN_IND", "HSHLD_INT_ARCHV_GENL_IND", 
                    "HSHLD_INT_ARCHV_ANTQ_IND", "HSHLD_INT_ARCHV_SKILLED_IND", "HSHLD_INT_ARCHV_COINS_IND", "HSHLD_INT_ARCHV_STKR_IND", "HSHLD_INT_CMPTR_IND", 
                    "HSHLD_INT_CONSMR_ELECTRNC_IND", "HSHLD_INT_PREP_NUTRTN_GENL_IND", "HSHLD_INT_PREP_SPECL_NUTRTN_IND", "HSHLD_INT_PREP_LOW_FAT_NUTRTN_IND", 
                    "HSHLD_INT_NTRL_NUTRTN_IND", "HSHLD_INT_VEG_IND", "HSHLD_INT_BUTY_PRODT_IND", "HSHLD_INT_STYLE_IND", "HSHLD_INT_WGHT_LOSS_IND", "HSHLD_INT_HLTH_ALRGY_RELT_IND", 
                    "HSHLD_INT_HLTH_MOBLTY_IND", "HSHLD_INT_HLTH_CHLSTRL_FOC_IND", "HSHLD_INT_HLTH_DIABTC_INT_IND", "HSHLD_INT_HLTH_IND", "HSHLD_INT_HLTH_BIOL_FOC_IND", 
                    "HSHLD_INT_HLTH_DR_INT_IND", "HSHLD_INT_HLTH_SNR_NEED_IND", "HSHLD_INT_HLTH_MED_IND", "HSHLD_INT_PERSNL_IMPRVMT_IND", "HSHLD_INT_CRAFT_IND", 
                    "HSHLD_INT_LAND_PROSPT_IND", "HSHLD_INT_HOME_FURN_IND", "HSHLD_INT_HOME_IMPRVMT_IND", "HSHLD_INT_MATRL_WRK_IND", "HSHLD_INT_LG_LIVE_IND", "HSHLD_INT_COM_LIVE_IND", 
                    "HSHLD_INT_CULTR_SKILLED_LIVE_IND", "HSHLD_INT_GREEN_LIVE_IND", "HSHLD_INT_HT_LIVE_IND", "HSHLD_INT_HOME_LIVE_IND", "HSHLD_INT_PROFL_LIVE_IND", 
                    "HSHLD_INT_ACTV_LIVE_IND", "HSHLD_INT_HIGH_LIVE_IND", "HSHLD_INT_PICTR_COLLCT_IND", "HSHLD_INT_PICTR_AT_HOME_IND", "HSHLD_INT_ENTMT_LSTNING_IND", 
                    "HSHLD_INT_ENTMT_HOME_IND", "HSHLD_INT_LSTNING_ENTMT_COLLCT_IND", "HSHLD_INT_ENTMT_DISPL_IND", "HSHLD_INT_CMPTR_EVENT_IND", "HSHLD_INT_VTO_EVENT_IND", 
                    "HSHLD_INT_EXRCS_IND", "HSHLD_INT_EXRCS_RUN_IND", "HSHLD_INT_EXRCS_XWALK_IND", "HSHLD_INT_BRD_EVENT_IND", "HSHLD_INT_ARWY_CRAFT_IND", 
                    "HSHLD_INT_WTRFL_EVENT_IND", "HSHLD_INT_OUTDR_ACTY_IND", "HSHLD_INT_PET_ANML_OWNR_IND", "HSHLD_INT_OTHR_PET_ANML_OWNR_IND", 
                    "HSHLD_INT_PET_OWNR_FOR_MORE_THAN_ONE_ANML_IND", "HSHLD_INT_PICTR_IND", "HSHLD_INT_REDWD_WRKNG_IND", "HSHLD_INT_MOTR_CYC_IND", 
                    "HSHLD_EVENT_INT_BSBLL_IND", "HSHLD_EVENT_INT_BSKTBLL_IND", "HSHLD_EVENT_INT_FTBLL_IND", "HSHLD_EVENT_INT_HCKY_IND", "HSHLD_EVENT_INT_SCCR_IND", 
                    "HSHLD_EVENT_INT_WTCHNG_TNNS_IND", "HSHLD_EVENT_INT_BKNG_IND", "HSHLD_EVENT_INT_RIDE_HRSS_IND","HSHLD_EVENT_INT_FSHNG_IND", "HSHLD_EVENT_INT_GLF_IND", 
                    "HSHLD_EVENT_INT_HUNTNG_IND", "HSHLD_EVENT_INT_SCBA DVNG_IND", "HSHLD_EVENT_INT_SNW SKNG_IND", "HSHLD_EVENT_INT_PLYNG_TNNS_IND", 
                    "HSHLD_INT_DOMSTC_TRVL_IND", "HSHLD_INT_ENTMT_VEH_IND", "HSHLD_INT_TRVL_VACATN_IND", "HSHLD_INT_TRVL_FAM_VACATN_IND", 
                    "HSHLD_INT_VACATN_TRVL_RCRTON_VEH_IND", "HSHLD_INT_VACATN_TRVL_CSNO_IND", "HSHLD_INT_VACATN_CSNO_ENJY_IND", "HSHLD_INT_VACATN_TRVL_TM_SHARE_HAVE_TAKEN_IND", 
                    "HSHLD_INT_VACATN_TM_SHARE_WOULD_ENJOY_IND", "HSHLD_INT_AUTO_WRK_IND", "HSHLD_INT_MOTRT_IND", "HSHLD_INT_PERSNL_INVEST_IND", 
                    "HSHLD_INT_REAL_LAND_INVEST_IND", "HSHLD_INT_FINANC_INVEST_IND"]
    
    dataAcxiomChangeDataTypeFinalDf = dataAcxiomhhintChangeDataTypeDf.toDF(*Data_list)

    return dataAcxiomChangeDataTypeFinalDf


# Add standard columns to the table
def addDefaultColumns(dataAcxiomhshldpctChangeDataTypeDf):
    acxiomHouseHoldHeaderDf = (
        dataAcxiomhshldpctChangeDataTypeDf.withColumn(
            "ACXIOM_HSHLD_PCT_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return acxiomHouseHoldHeaderDf

# Prepare an hhintidual table
def prepareHouseHoldpctChangeDataType(dataAcxiomhshldpctChangeDataTypeDf):  

    houseHoldTempDf = addDefaultColumns(dataAcxiomhshldpctChangeDataTypeDf)
    data_acxiom_hhpct_hkc = addhousekeepingColumns(data_acxiom)
    
    dataAcxiomhshldpctChangeDataTypeDf = houseHoldTempDf.select(
                                (houseHoldTempDf.ACXIOM_HSHLD_PCT_DTL_KEY.cast(StringType())),
                                (data_acxiom_hhpct_hkc.CRET_TS.cast(TimestampType())),
                                (data_acxiom_hhpct_hkc.CHNL_CD.cast(StringType())),
                                (data_acxiom_hhpct_hkc.CHNL_SRC_CD.cast(StringType())),
                                (data_acxiom_hhpct_hkc.ADDR_LNK_KEY.cast(StringType())),
                                (data_acxiom_hhpct_hkc.HSHLD_LNK_KEY.cast(StringType())),
                                (data_acxiom_hhpct_hkc.CURR_RCD_IND.cast(StringType())),
                                (houseHoldTempDf.miACS_11_002.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_001.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_045.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_047.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_048.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_049.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_050.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_051.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_052.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_053.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_054.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_055.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_037.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_019.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_020.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_021.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_022.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_023.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_027.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_024.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_025.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_026.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_030.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_031.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_029.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_035.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_040.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_032.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_036.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_033.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_041.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_043.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_042.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_034.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_028.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_012.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_006.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_011.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_038.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_10_002.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_014.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_007.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_013.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_009.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_016.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_008.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_015.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_003.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_004.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_11_005.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_017.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_018.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_11_010.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_021.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_005.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_022.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_024.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_023.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_007.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_009.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_025.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_008.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_010.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_012.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_011.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_013.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_016.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_014.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_015.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_017.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_019.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_018.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_020.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_004.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_006.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_042.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_047.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_045.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_046.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_041.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_043.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_044.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_19_040.cast(DecimalType(5,2))),
                                (houseHoldTempDf.miACS_17_002.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_22_001.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_16_008.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_08_027.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_08_030.cast(DecimalType(4,2))),
                                (houseHoldTempDf.miACS_08_029.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_08_031.cast(DecimalType(3,2))),
                                (houseHoldTempDf.miACS_08_026.cast(DecimalType(3,2)))

    )
    
    return dataAcxiomhshldpctChangeDataTypeDf


# Update column names as per data dictionary
def updateColumnNamesInHouseHoldpctTable(dataAcxiomhshldpctChangeDataTypeDf):    
    Data_list = ["ACXIOM_HSHLD_PCT_DTL_KEY", "CRET_TS", "CHNL_CD", "CHNL_SRC_CD", "ADDR_LNK_KEY", "HSHLD_LNK_KEY", "CURR_RCD_IND", "HSHLD_FEM_PCT", "HSHLD_MLE_PCT", 
                    "HSHLD_MEDN_PERSN_CNT", "HSHLD_1_PERSN_PCT", "HSHLD_2_PERSN_PCT", "HSHLD_3_PERSN_PCT", "HSHLD_OVR_3_PERSN_PCT", "HSHLD_4_PERSN_PCT", 
                    "HSHLD_OVR_4_PERSN_PCT", "HSHLD_5_PERSN_PCT", "HSHLD_OVR_5_PCT", "HSHLD_OVR_6_PERSN_PCT", "HSHLD_FAM_PCT", 
                    "HSHLD_PERSN_AGE_15_BTWN_24_PCT", "HSHLD_PERSN_AGE_25_BTWN_34_PCT", "HSHLD_PERSN_AGE_35_BTWN_44_PCT", "HSHLD_PERSN_AGE_45_BTWN_54_PCT", 
                    "HSHLD_PERSN_AGE_55_BTWN_64_PCT", "HSHLD_PERSN_AGE_OVR_65_PCT", "HSHLD_PERSN_AGE_65_BTWN_74_PCT", "HSHLD_PERSN_AGE_75_BTWN_84_PCT", "HSHLD_PERSN_AGE_85_OVR_PCT", 
                    "HSHLD_RACE_AMRCN_INDN_NTV_ALONE_PCT", "HSHLD_RACE_ASIAN_ALONE_PCT", "HSHLD_RACE_BLACK_ALONE_PCT", "HSHLD_RACE_HISP_ALONE_PCT", "HSHLD_LIVE_WTH_SPS_PCT", 
                    "HSHLD_RACE_HWAN_ALONE_PCT", "HSHLD_RACE_NOT_HISP_ALONE_PCT", "HSHLD_OTHR_RACE_ALONE_PCT", "HSHLD_SNGL_PCT", 
                    "HSHLD_SNGL_FEM_PCT", "HSHLD_SNGL_MLE_PCT", "HSHLD_OVR_2_RACE_PCT", "HSHLD_RACE_WHT_ALONE_PCT", "HSHLD_WTH_SPS_NO_CHLDRN_PCT", "HSHLD_FAM_CHLDRN_PCT", 
                    "HSHLD_FAM_RELT_CHLDRN_PCT", "HSHLD_NOT_FAM_PCT", "HSHLD_GRNDCHLDRN_UND_18_RESPBL_PCT", "HSHLD_SNGL_NO_CHLDRN_PCT", "HSHLD_SNGL_CHLDRN_PCT", 
                    "HSHLD_SNGL_RELT_CHLDRN_PCT", "HSHLD_SNGL_FEM_CHLDRN_PCT", "HSHLD_SNGL_FEM_RELT_CHLDRN_PCT", "HSHLD_SNGL_MLE_CHLDRN_PCT", "HSHLD_SNGL_MLE_RELT_CHLDRN_PCT", 
                    "HSHLD_CHLDRN_PCT", "HSHLD_NO_CHLDRN_PCT", "HSHLD_OWN_CHLDRN_PCT", "HSHLD_AGE_OVR_65_PCT", "HSHLD_AGE_OVR_65_LIVE_ALONE_PCT", "HSHLD_RELT_CHLDRN_PCT", 
                    "HSHLD_DOLLR_INCM_100000_BTWN_124999_PCT", "HSHLD_DOLLR_INCM_10000_BTWN_14999_PCT", "HSHLD_DOLLR_INCM_125000_BTWN_149999_PCT", "HSHLD_DOLLR_INCM_150000_OVR_PCT", "HSHLD_DOLLR_INCM_150000_BTWN_199999_PCT", 
                    "HSHLD_DOLLR_INCM_15000_BTWN_19999_PCT", "HSHLD_DOLLR_INCM_15000_BTWN_24999_PCT", "HSHLD_DOLLR_INCM_200000_OVR_PCT", "HSHLD_DOLLR_INCM_20000_BTWN_24999_PCT", "HSHLD_DOLLR_INCM_25000_BTWN_29999_PCT", 
                    "HSHLD_DOLLR_INCM_25000_BTWN_34999_PCT", "HSHLD_DOLLR_INCM_30000_BTWN_34999_PCT", "HSHLD_DOLLR_INCM_35000_BTWN_39999_PCT", "HSHLD_DOLLR_INCM_35000_BTWN_49999_PCT", "HSHLD_DOLLR_INCM_40000_BTWN_44999_PCT", 
                    "HSHLD_DOLLR_INCM_45000_BTWN_49999_PCT", "HSHLD_DOLLR_INCM_50000_BTWN_59999_PCT", "HSHLD_DOLLR_INCM_50000_BTWN_74999_PCT", "HSHLD_DOLLR_INCM_60000_BTWN_74999_PCT", "HSHLD_DOLLR_INCM_75000_BTWN_99999_PCT", 
                    "HSHLD_LT_DOLLR_INCM_10000_PCT", "HSHLD_LT_DOLLR_INCM_15000_PCT", "HSHLD_INT_RNTL_DVDND_INCM_PCT", "HSHLD_OTHR_INCM_PCT", 
                    "HSHLD_PUB_ASSTNC_INCM_PCT", "HSHLD_RETRMT_INCM_PCT", "HSHLD_OWN_EMPLMNT_INCM_PCT", "HSHLD_SS_INCM_PCT", 
                    "HSHLD_SUPLMTL_SECRTY_INCM_PCT", "HSHLD_SAL_INCM_PCT", "HSHLD_UND_PVRTY_LVL_PCT", "HSHLD_RECV_FD_STMP_PCT", "HSHLD_LANG_ISLT_PCT", 
                    "HSHLD_1_VEH_PCT", "HSHLD_OVR_2_VEH_PCT", "HSHLD_2_VEH_PCT","HSHLD_OVR_3_VEH_PCT", "HSHLD_NO_VEH_PCT"
                    ]
    
    dataAcxiomChangeDataTypeFinalDf = dataAcxiomhshldpctChangeDataTypeDf.toDF(*Data_list)

    return dataAcxiomChangeDataTypeFinalDf


# Add standard columns to the table
def addDefaultColumns(data_acxiom_age):
    data_acxiom_age = (
        data_acxiom_age.withColumn(
            "ACXIOM_AGE_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return data_acxiom_age

# Prepare an age table
def prepareAcxiomAgeTable(data_acxiom_age):
    
    data_acxiom_age_upd = addDefaultColumns(data_acxiom_age)
    data_acxiom_age_hkc = addhousekeepingColumns(data_acxiom)
    
    data_acxiom_age_new = data_acxiom_age_upd.select(
        (data_acxiom_age_upd.ACXIOM_AGE_DTL_KEY.cast(StringType())),
        (data_acxiom_age_hkc.CRET_TS.cast(TimestampType())),
        (data_acxiom_age_hkc.CHNL_CD.cast(StringType())),
        (data_acxiom_age_hkc.CHNL_SRC_CD.cast(StringType())),
        (data_acxiom_age_hkc.ADDR_LNK_KEY.cast(StringType())),
        (data_acxiom_age_hkc.HSHLD_LNK_KEY.cast(StringType())),
        (data_acxiom_age_hkc.CURR_RCD_IND.cast(StringType())),    
        (data_acxiom_age_upd.IBE8600_01.cast(ByteType())),
        (data_acxiom_age_upd.IBE8600_02.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_03.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_04.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_05.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_06.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_07.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_08.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_09.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_10.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_11.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_12.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_13.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_14.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_15.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_16.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_17.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_18.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_19.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_20.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8600_21.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_01.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_02.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_03.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_04.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_05.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_06.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_07.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_08.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_09.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_10.cast(ByteType())),   
        (data_acxiom_age_upd.IBE8601_11.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_12.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_13.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_14.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8601_15.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_01.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_02.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_03.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_04.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_05.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_06.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_07.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_08.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_09.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_10.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_11.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_12.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_13.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_14.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_15.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_16.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_17.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_18.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_19.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_20.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7600_21.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_01.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_02.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_03.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_04.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_05.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_06.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_07.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_08.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_09.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_10.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_11.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_12.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_13.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_14.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_15.cast(ByteType())), 
        (data_acxiom_age_upd.IBE7601_16.cast(StringType())), 
        (data_acxiom_age_upd.IBE7600_22.cast(StringType())), 
        (data_acxiom_age_upd.IBE8603_01.cast(StringType())), 
        (data_acxiom_age_upd.IBE8603_02.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_03.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_04.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_05.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_06.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_07.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_08.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_09.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_10.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_11.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_12.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_13.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_14.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_15.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_16.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_17.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_18.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8603_19.cast(StringType())), 
        (data_acxiom_age_upd.IBE8627_01.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8626.cast(ByteType())),
        (data_acxiom_age_upd.IBE8616.cast(ByteType())), 
        (data_acxiom_age_upd.IBE9616.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8682.cast(ByteType())), 
        (data_acxiom_age_upd.IBE8686.cast(StringType())), 
        (data_acxiom_age_upd.IBE8690.cast(StringType())), 
        (data_acxiom_age_upd.IBE8627_02.cast(StringType())), 
        (data_acxiom_age_upd.IBE8617.cast(ByteType())), 
        (data_acxiom_age_upd.miACS_01_032.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_033.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_034.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_035.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_036.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_037.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_038.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_026.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_027.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_020.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_022.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_021.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_025.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_023.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_024.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_01_019.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_14_005.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_14_004.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_014.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_005.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_004.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_001.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_013.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_016.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_002.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_003.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_007.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_011.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_010.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_008.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_009.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_006.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_012.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_015.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_017.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_018.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_019.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_024.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_020.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_021.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_025.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_022.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_023.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_23_011.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_23_014.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_23_012.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_23_013.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_003.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_002.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_001.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_004.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_005.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_007.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_006.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_21_008.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_032.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_033.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_034.cast(DecimalType(3,2))), 
        (data_acxiom_age_upd.miACS_08_028.cast(DecimalType(3,2)))  

    )
    
    return data_acxiom_age_new

# Update column names as per data dictionary
def updateColumnNamesInAgeTable(data_acxiom_age):
    
    Data_list = ["ACXIOM_AGE_DTL_KEY", 
                 "CRET_TS", 
                 "CHNL_CD", 
                 "CHNL_SRC_CD", 
                 "ADDR_LNK_KEY", 
                 "HSHLD_LNK_KEY",
                 "CURR_RCD_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_18_24_MALE_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_18_24_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_18_24_UNKN_GENDR_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_25_34_MALE_IND",  
                 "ADULT_AGE_RNG_PRES_BTWN_25_34_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_25_34_UNKN_GENDR_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_35_44_MALE_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_35_44_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_35_44_UNKN_GENDR_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_45_54_MALE_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_45_54_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_45_54_UNKN_GENDR_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_55_64_MALE_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_55_64_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_55_64_UNKN_GENDR_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_65_74_MALE_IND",
                 "ADULT_AGE_RNG_PRES_BTWN_65_74_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_BTWN_65_74_UNKN_GENDR_IND", 
                 "ADULT_AGE_RNG_PRES_75_OVR_MALE_IND", 
                 "ADULT_AGE_RNG_PRES_75_OVR_FEM_IND", 
                 "ADULT_AGE_RNG_PRES_75_OVR_UNKN_GENDR_IND", 
                 "CHLDRN_AGE_RNG_BTWN_00_02_MALE_IND", 
                 "CHLDRN_AGE_RNG_BTWN_00_02_FEM_IND", 
                 "CHLDRN_AGE_RNG_BTWN_00_02_UNKN_GENDR_IND", 
                 "CHLDRN_AGE_RNG_BTWN_03_05_MALE_IND", 
                 "CHLDRN_AGE_RNG_BTWN_03_05_FEM_IND", 
                 "CHLDRN_AGE_RNG_BTWN_03_05_UNKN_GENDR_IND", 
                 "CHLDRN_AGE_RNG_BTWN_06_10_MALE_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_06_10_FEM_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_06_10_UNKN_GENDR_IND",
                 "CHLDRN_AGE_RNG_PRES_BTWN_11_15_MALE_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_11_15_FEM_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_11_15_UNKN_GENDR_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_16_17_MALE_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_16_17_FEM_IND", 
                 "CHLDRN_AGE_RNG_PRES_BTWN_16_17_UNKN_GENDR_IND",
                 "ADLT_100_PCT_AGE_RNG_PRES_BTWN_18_24_MALE_IND", 
                 "ADLT_100_PCT_AGE_RNG_PRES_BTWN_18_24_FEM_IND",
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_18_24_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_25_34_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_25_34_FEM_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_25_34_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_35_44_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_35_44_FEM_IND",
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_35_44_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_45_54_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_45_54_FEM_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_45_54_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_55_64_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_55_64_FEM_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_55_64_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_65_74_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_65_74_FEM_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_BTWN_65_74_UNKN_GENDR_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_75_OVR_MALE_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_75_OVR_FEM_IND", 
                 "ADULT_100_PCT_AGE_RNG_PRES_75_OVR_UNKN_GENDR_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_00_02_MALE_IND",
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_00_02_FEM_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_00_02_UNKN_GENDR_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_03_05_MALE_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_03_05_FEM_IND",
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_03_05_UNKN_GENDR_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_06_10_MALE_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_06_10_FEM_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_06_10_UNKN_GENDR_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_11_15_MALE_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_11_15_FEM_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_11_15_UNKN_GENDR_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_16_17_MALE_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_16_17_FEM_IND", 
                 "CHLDRN_100_PCT_AGE_RNG_PRES_BTWN_16_17_UNKN_GENDR_IND",
                 "CHLDRN_100_PCT_AGE_RNG_PRES_PRCSN_LVL_CD", 
                 "ADULT_100_PCT_AGE_RNG_PRES_PRCSN_LVL_CD",
                 "CHLDRN_AGE_1_YR_INCRMT_LT_AGE_01_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_01_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_02_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_03_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_04_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_05_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_06_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_07_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_08_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_09_IND",
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_10_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_11_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_12_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_13_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_14_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_15_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_16_IND", 
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_17_IND",
                 "CHLDRN_AGE_1_YR_INCRMT_AGE_18_IND",
                 "AGE_DEFLT_FRST_PERSN_IN_2_YR_INCRMT_CD",
                 "AGE_PERSN_IN_2_YR_INCRMT_CD", 
                 "AGE_RANK_FRST_PERSN_IN_HH_2_YR_INCRMT_CD", 
                 "AGE_RANK_FRST_PERSN_IN_HH_PLUS_IN_2_YR_INCRMT_CD", 
                 "AGE_RANK_THRD_PERSN_IN_HH_2_YR_INCRMT_CD", 
                 "AGE_RANK_FRTH_PERSN_IN_HH_2_YR_INCRMT_CD", 
                 "AGE_RANK_FIFTH_PERSN_IN_HH_2_YR_INCRMT_CD",
                 "AGE_DEFLT_FRST_PERSN_IN_HSHLD_PRCSN_CD", 
                 "AGE_IN_2_YR_INCRMT_SEC_PERSN_IN_HSHLD_CD", 
                 "ADULT_AGE_GT_18_AND_IN_BTWN_18_AND_24_PCT", 
                 "ADULT_AGE_GT_18_AND_IN_BTWN_25_AND_34_PCT",
                 "ADULT_AGE_GT_18_AND_IN_BTWN_35_AND_44_PCT", 
                 "ADULT_AGE_GT_18_AND_IN_BTWN_45_AND_54_PCT",
                 "ADULT_AGE_GT_18_AND_IN_BTWN_55_AND_64_PCT", 
                 "ADULT_AGE_GT_18_AND_IN_BTWN_65_AND_74_PCT",
                 "ADULT_AGE_GT_18_AND_GT_75_PCT", 
                 "CHLDRN_AGE_IN_BTWN_12_TO_14_AND_LT_18_PCT",
                 "CHLDRN_AGE_IN_BTWN_15_TO_17_AND_LT_18_PCT", 
                 "CHLDRN_AGE_IN_BTWN_3_TO_4_AND_LT_18_PCT",
                 "CHLDRN_AGE_IN_BTWN_3_TO_5_AND_LT_18_PCT", 
                 "CHLDRN_AGE_OF_5_AND_LT_18_AND_PCT",
                 "CHLDRN_AGE_IN_BTWN_6_TO_11_AND_LT_18_PCT", 
                 "CHLDRN_AGE_IN_BTWN_6_TO_8_AND_LT_18_PCT", 
                 "CHLDRN_AGE_IN_BTWN_9_TO_11_AND_LT_18_PCT", 
                 "CHLDRN_AGE_BELOW_3_AND_LT_18_PCT",
                 "CHLDRN_AGE_GT_3_ENRL_IN_PRVT_SCHL_PCT", 
                 "CHLDRN_AGE_GT_3_ENRL_IN_PUB_SCHL_PCT", 
                 "WRKR_AGE_GT_16_CYC_PCT", 
                 "WRKR_AGE_GT_16_CAR_SHARE_PCT",
                 "WRKR_AGE_GT_16_CAR_OR_TRUCK_OR_VAN_DRVR_ALONE_PCT", 
                 "WRKR_AGE_GT_16_IN_CNTY_OF_RSDNC_PCT", 
                 "WRKR_AGE_GT_16_MOTR_CYC_PCT",
                 "WRKR_AGE_GT_16_OTHR_FRM_OF_TRNSPRTN_PCT", 
                 "WRKR_AGE_GT_16_OUTSD_CNTY_OF_RSDNC_PCT", 
                 "WRKR_AGE_GT_16_OUTSD_ST_OF_RSDNC_PCT", 
                 "WRKR_AGE_GT_16_TAKE_PUB_TRNSPRTN_PCT",
                 "WRKR_AGE_GT_16_TAKE_SHIP_PCT", 
                 "WRKR_AGE_GT_16_PUB_TRN_TRNSPRTN_PCT", 
                 "WRKR_AGE_GT_16_ST_VEH_PCT", 
                 "WRKR_AGE_GT_16_SUB_PCT", 
                 "WRKR_AGE_GT_16_EXCL_TAXI_PCT", 
                 "WRKR_AGE_GT_16_TAXI_PCT", 
                 "WRKR_AGE_GT_16_PCT", 
                 "WRKR_AGE_GT_16_WRK_HOME_PCT", 
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_AVG_IN_MIN_NUM", 
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_MEDN_IN_MIN_NUM",
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_BTWN_15_AND_59_MIN_PCT",
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_BTWN_1_AND_29_MIN_PCT", 
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_BTWN_30_AND_59_MIN_PCT", 
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_60_OVR_MIN_PCT",
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_BTWN_60_AND_89_MIN_PCT", 
                 "WRKR_AGE_GT_16_NOT_WRKNG_AT_HOME_90_OVR_MIN_PCT", 
                 "OWN_CHLDRN_PRNT_IN_WRKNG_FRC_PCT",
                 "OWN_CHLDRN_NO_PRNT_IN_WRKNG_FRC_PCT", 
                 "OWN_CHLDRN_SNGL_NOT_MTHR_IN_WRKNG_FRC_PCT", 
                 "OWN_CHLDRN_SNGL_MTHR_IN_WRKNG_FRC_PCT", 
                 "FEM_18_OVR_VET_PCT",
                 "MALE_18_OVR_VET_PCT", 
                 "VET_18_PLUS_PCT", 
                 "VET_18_PLUS_AFTR_SEP_2001_ONLY_PCT", 
                 "VET_18_PLUS_GULF_WAR_1_PCT", 
                 "VET_18_KOREA_WAR_PCT", 
                 "VET_18_PLUS_VIETNAM_WAR_PCT", 
                 "VET_18_PLUS_WW_WAR_2_PCT",
                 "HSHLD_BTWN_15_AND_34_AND_OVR_1_VEH_PCT", 
                 "HSHLD_BTWN_35_AND_64_AND_OVR_1_VEH_PCT", 
                 "HSHLD_AGE_GT_65_AND_OVR_1_VEH_PCT", 
                 "HSHLD_AGE_OVR_1_VEH_PCT"]
        
    data_acxiom_age_final = data_acxiom_age.toDF(*Data_list)
                                        
    return data_acxiom_age_final


# Add standard columns to the table
def addDefaultColumns(data_acxiom_indiv):
    data_acxiom_indiv_upd = (
        data_acxiom_indiv.withColumn(
            "ACXIOM_INDIV_DTL_KEY", col("INPUT_ABILITEC_CONSUMER_LINK_16BYTE")
        )
    )

    return data_acxiom_indiv_upd

# Prepare an individual table
def prepareAcxiomIndividualTable(data_acxiom_indiv):  
    data_acxiom_indiv_upd = addDefaultColumns(data_acxiom_indiv)
    data_acxiom_indiv_hkc = addhousekeepingColumns(data_acxiom)
    
    data_acxiom_indiv_new = data_acxiom_indiv_upd.select(
        (data_acxiom_indiv_upd.ACXIOM_INDIV_DTL_KEY.cast(StringType())),
        (data_acxiom_indiv_hkc.CRET_TS.cast(TimestampType())),
        (data_acxiom_indiv_hkc.CHNL_CD.cast(StringType())),
        (data_acxiom_indiv_hkc.CHNL_SRC_CD.cast(StringType())),
        (data_acxiom_indiv_hkc.ADDR_LNK_KEY.cast(StringType())),
        (data_acxiom_indiv_hkc.HSHLD_LNK_KEY.cast(StringType())),
        (data_acxiom_indiv_hkc.CURR_RCD_IND.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_FIRST_NAME_3131.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_MIDDLE_INITIAL_3131.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_LAST_NAME_3131.cast(StringType())),
        (data_acxiom_indiv_upd.ADDRESS_COMPRESSED_3132.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_ADDRESS_LINE2_4111.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_STREET_NAME_SUFFIX_POST_DIRECTIONAL_3133.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_STREET_DIRECTIONAL_3133.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_STREET_NUMBER_LEFTJUSTIFIED_3133.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_CITY_3040.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_STATE_3038.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_ZIP_CODE_3043.cast(ByteType())),
        (data_acxiom_indiv_upd.INPUT_ZIP4_CODE_3044.cast(ShortType())),
        (data_acxiom_indiv_upd.INPUT_PREFIX_TITLE_8093.cast(StringType())),
        (data_acxiom_indiv_upd.INPUT_USE_CODE_3048.cast(ByteType())),
        (data_acxiom_indiv_upd.INPUT_DELIVERY_POINT_BAR_CODE_3046.cast(ShortType())),
        (data_acxiom_indiv_upd.IBE8688.cast(StringType())),
        (data_acxiom_indiv_upd.IBE8623_03.cast(ShortType())),
        (data_acxiom_indiv_upd.IBE8623_02.cast(ShortType())),
        (data_acxiom_indiv_upd.IBE8623_01.cast(ShortType())),
        (data_acxiom_indiv_upd.IBE8637.cast(StringType())),
        (data_acxiom_indiv_upd.IBE2360.cast(StringType())),
        (data_acxiom_indiv_upd.IBE9514.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE8531.cast(StringType())),
        (data_acxiom_indiv_upd.IBE9533.cast(StringType())),
        (data_acxiom_indiv_upd.IBE2807.cast(StringType())),
        (data_acxiom_indiv_upd.IBE7825.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7832.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7829.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7826.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7827.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7830.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7828.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7823.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7822.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7824.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE7831.cast(ByteType())),
        (data_acxiom_indiv_upd.IBE6603.cast(IntegerType())),
        (data_acxiom_indiv_upd.IBE6611.cast(IntegerType())),
        (data_acxiom_indiv_upd.IBE2526.cast(ByteType()))
    )
    
    return data_acxiom_indiv_new


# Update column names as per data dictionary
def updateColumnNamesInAcxiomIndividualTable(data_acxiom_indiv):    
    Data_list = ["ACXIOM_INDIV_DTL_KEY", "CRET_TS", "CHNL_CD", "CHNL_SRC_CD", "ADDR_LNK_KEY", "HSHLD_LNK_KEY", "CURR_RCD_IND","FRST_NM", "MID_INIT_NM", "LAST_NM", "ADDR_LN_1", "ADDR_LN_2", "ST_NM", "ST_DIRCTN_CD", "ADDR_COMPRS_TXT", "CITY_NM", "ST_CD", "POSTL_CD", "POSTL_EXT_CD", "PFX_TXT", "USE_NUM", "DLVRY_PNT_NUM", "GENDR_CD", "BRTH_DAY_INDIV_NUM", "BRTH_MTH_INDIV_NUM", "BRTH_YR_INDIV_NUM", "OCCUP_PERSN_CD", "OCCUP_DTL_PERSN_CD", "EDUCN_PERSN_CD", "PLTCL_PARTY_PERSN_CD", "RACE_PERSN_CD", "BUS_OWNR_PERSN_CD", "STUDY_GRP_IND", "ANTQ_GRP_IND", "ELECTRNC_CMPT_GRP_IND", "ARNG_NUTRTN_IND", "EXRCS_HLTH_GRP_IND", "HOME_IMPRVMT_GRP_IND", "ENTMT_GRP_IND", "OUTSD_ACTY_GRP_IND", "PHY_ACTY_GRP_IND", "TRVL_GRP_IND", "FUND_OR_FINANC_GRP_IND", "TOTL_MTHD_OF_PAYMT_AMEX_CARD_CNT", "TOTL_MTHD_OF_PAYMT_VSTR_CARD_CNT", "INFER_RANK_IN_HSHLD"]
    data_acxiom_indiv_final = data_acxiom_indiv.toDF(*Data_list)

    return data_acxiom_indiv_final


		
if __name__ == '__main__':
	st = time.time()
	# Reading data axle data from edp raw zone bucket.
	logger.info(f'Reading data from raw zone: {edp_raw_zone_bucket} started...')
	dataAxleDF = readRawZone(spark)
	logger.info(f"dataAxleDF number of columns =====>{len(dataAxleDF.columns)}")

    if(tableName == 'ACXIOM_POPTN_DTL'):

    	# Reading only acxiom population data.
    	logger.info(f'Reading acxiom population columns started...')

    	acxiom_poptn_tbl = prepareAcxiomPoptnTable(data_acxiom_df)
        acxiom_poptn_tbl_upd = updateColumnNamesInPoptnTable(acxiom_poptn_tbl)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_POPTN_DTL/"
        acxiom_poptn_tbl_upd.write.mode("overwrite").options(delimiter="|").csv(bucketName)


    elif(tableName == 'ACXIOM_HSHLD_PURCHS_DTL'):

    	# Reading only house hold purchase data.
    	logger.info(f'Reading house hold purchase columns started...')

        acxiom_hhpurchase_tbl = prepareHouseHoldpurchsChangeDataType(data_acxiom_df)
        acxiom_hhpurchase_upd = updateColumnNamesInHouseHoldpurchsTable(acxiom_hhpurchase_tbl)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_HSHLD_PURCHS_DTL/"
        acxiom_hhpurchase_upd.write.mode("overwrite").options(delimiter="|").csv(bucketName)


    elif(tableName == 'ACXIOM_HSHLD_INT_DTL'):


    	# Reading only house hold interest data.
    	logger.info(f'Reading house hold intrerest columns started...')

        dataAcxiomhhintChangeDataTypeDf = prepareHouseHoldintChangeDataType(data_acxiom_df)
        dataAcxiomChangeDataTypeFinalDf = updateColumnNamesInHouseHoldintTable(dataAcxiomhhintChangeDataTypeDf)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_HSHLD_INT_DTL/"
        dataAcxiomChangeDataTypeFinalDf.write.mode("overwrite").options(delimiter="|").csv(bucketName)


    elif(tableName == 'ACXIOM_HSHLD_PCT_DTL'):


    	# Reading only house hold percent profile data.
    	logger.info(f'Reading house hold percent columns started...')

        dataAcxiomhshldpctChangeDataTypeDf = prepareHouseHoldpctChangeDataType(data_acxiom_df)
        dataAcxiomChangeDataTypeFinalDf = updateColumnNamesInHouseHoldpctTable(dataAcxiomhshldpctChangeDataTypeDf)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_HSHLD_PCT_DTL/"
        dataAcxiomChangeDataTypeFinalDf.write.mode("overwrite").options(delimiter="|").csv(bucketName)


    elif(tableName == 'ACXIOM_AGE_DTL'):


    	# Reading only age details data.
    	logger.info(f'Reading age detail columns started...')

        acxiom_age_tbl = prepareAcxiomAgeTable(data_acxiom_df)
        acxiom_age_tbl_upd = updateColumnNamesInAgeTable(acxiom_age_tbl)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_AGE_DTL/"
        acxiom_age_tbl_upd.write.mode("overwrite").options(delimiter="|").csv(bucketName)


    elif(tableName == 'ACXIOM_INDIV_DTL'):


    	# Reading only individual profile data.
    	logger.info(f'Reading individual profile columns started...')

        acxiom_indiv_table = prepareAcxiomIndividualTable(data_acxiom_df)
        acxiom_indiv_table_upd = updateColumnNamesInAcxiomIndividualTable(acxiom_indiv_table)

        bucketName = "s3://" + edp_data_product_bucket + "/PROSPECT/acxiom/ACXIOM_INDIV_DTL/"
        acxiom_indiv_table_upd.write.mode("overwrite").options(delimiter="|").csv(bucketName)



    st = time.time()
    logger.info(f"Writing mapped profile data into data product bucket {edp_data_product_bucket} started...")
    logger.info(f"Writing mapped profile data into data product bucket completed.")
    # get the end time
    et = time.time()
    # get the execution time
    elapsed_time = et - st
    logger.info('Execution time for writing data:', elapsed_time, 'seconds')
    logger.info(f'Exiting job.')