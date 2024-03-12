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

# Script generated for node customer_landing S3 source
customer_landingS3source_node1710255634662 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={"paths": ["s3://jan-lake-house/customer/landing/"]},
        transformation_ctx="customer_landingS3source_node1710255634662",
    )
)

# Script generated for node filter trusted customer
SqlQuery395 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;
"""
filtertrustedcustomer_node1710255666609 = sparkSqlQuery(
    glueContext,
    query=SqlQuery395,
    mapping={"customer_landing": customer_landingS3source_node1710255634662},
    transformation_ctx="filtertrustedcustomer_node1710255666609",
)

# Script generated for node customer_trusted S3 target
customer_trustedS3target_node1710255848667 = glueContext.getSink(
    path="s3://jan-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trustedS3target_node1710255848667",
)
customer_trustedS3target_node1710255848667.setCatalogInfo(
    catalogDatabase="jan", catalogTableName="customer_trusted"
)
customer_trustedS3target_node1710255848667.setFormat("json")
customer_trustedS3target_node1710255848667.writeFrame(
    filtertrustedcustomer_node1710255666609
)
job.commit()
