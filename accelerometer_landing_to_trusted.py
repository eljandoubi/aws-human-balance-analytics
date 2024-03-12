import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing S3 source
accelerometer_landingS3source_node1710258137086 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="accelerometer_landingS3source_node1710258137086",
    )
)

# Script generated for node customer_trusted S3 source
customer_trustedS3source_node1710258081122 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/customer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="customer_trustedS3source_node1710258081122",
    )
)

# Script generated for node accelerometer filter trusted
SqlQuery23 = """
select accelerometer_landing.* 
from accelerometer_landing
INNER JOIN customer_trusted on customer_trusted.email=accelerometer_landing.user;

"""
accelerometerfiltertrusted_node1710258177452 = sparkSqlQuery(
    glueContext,
    query=SqlQuery23,
    mapping={
        "customer_trusted": customer_trustedS3source_node1710258081122,
        "accelerometer_landing": accelerometer_landingS3source_node1710258137086,
    },
    transformation_ctx="accelerometerfiltertrusted_node1710258177452",
)

# Script generated for node accelerometer_trusted S3 target
accelerometer_trustedS3target_node1710258370819 = glueContext.getSink(
    path="s3://jan-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trustedS3target_node1710258370819",
)
accelerometer_trustedS3target_node1710258370819.setCatalogInfo(
    catalogDatabase="jan", catalogTableName="accelerometer_trusted"
)
accelerometer_trustedS3target_node1710258370819.setFormat("json")
accelerometer_trustedS3target_node1710258370819.writeFrame(
    accelerometerfiltertrusted_node1710258177452
)
job.commit()
