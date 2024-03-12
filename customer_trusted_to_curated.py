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

# Script generated for node accelerometer_trusted S3 source
accelerometer_trustedS3source_node1710258137086 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="accelerometer_trustedS3source_node1710258137086",
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

# Script generated for node customer filter curated
SqlQuery130 = """
select DISTINCT customer_trusted.* 
from customer_trusted
INNER JOIN accelerometer_trusted on customer_trusted.email=accelerometer_trusted.user;
"""
customerfiltercurated_node1710258177452 = sparkSqlQuery(
    glueContext,
    query=SqlQuery130,
    mapping={
        "customer_trusted": customer_trustedS3source_node1710258081122,
        "accelerometer_trusted": accelerometer_trustedS3source_node1710258137086,
    },
    transformation_ctx="customerfiltercurated_node1710258177452",
)

# Script generated for node customer_curated S3 target
customer_curatedS3target_node1710258370819 = glueContext.getSink(
    path="s3://jan-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curatedS3target_node1710258370819",
)
customer_curatedS3target_node1710258370819.setCatalogInfo(
    catalogDatabase="jan", catalogTableName="customer_curated"
)
customer_curatedS3target_node1710258370819.setFormat("json")
customer_curatedS3target_node1710258370819.writeFrame(
    customerfiltercurated_node1710258177452
)
job.commit()
