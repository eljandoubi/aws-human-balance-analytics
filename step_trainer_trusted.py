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

# Script generated for node step_trainer_landing S3 source
step_trainer_landingS3source_node1710258137086 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="step_trainer_landingS3source_node1710258137086",
    )
)

# Script generated for node customer_curated S3 source
customer_curatedS3source_node1710258081122 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/customer/curated/"],
            "recurse": True,
        },
        transformation_ctx="customer_curatedS3source_node1710258081122",
    )
)

# Script generated for node step_trainer filter trusted
SqlQuery42 = """
select DISTINCT step_trainer_landing.* 
from step_trainer_landing
INNER JOIN customer_curated on customer_curated.serialnumber=step_trainer_landing.serialnumber;
"""
step_trainerfiltertrusted_node1710258177452 = sparkSqlQuery(
    glueContext,
    query=SqlQuery42,
    mapping={
        "customer_curated": customer_curatedS3source_node1710258081122,
        "step_trainer_landing": step_trainer_landingS3source_node1710258137086,
    },
    transformation_ctx="step_trainerfiltertrusted_node1710258177452",
)

# Script generated for node customer_curated S3 target
customer_curatedS3target_node1710258370819 = glueContext.getSink(
    path="s3://jan-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curatedS3target_node1710258370819",
)
customer_curatedS3target_node1710258370819.setCatalogInfo(
    catalogDatabase="jan", catalogTableName="step_trainer_trusted"
)
customer_curatedS3target_node1710258370819.setFormat("json")
customer_curatedS3target_node1710258370819.writeFrame(
    step_trainerfiltertrusted_node1710258177452
)
job.commit()
