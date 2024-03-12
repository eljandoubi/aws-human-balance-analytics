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

# Script generated for node step_trainer_trusted S3 source
step_trainer_trustedS3source_node1710258137086 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/step_trainer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="step_trainer_trustedS3source_node1710258137086",
    )
)

# Script generated for node accelerometer_trusted S3 source
accelerometer_trustedS3source_node1710258081122 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://jan-lake-house/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="accelerometer_trustedS3source_node1710258081122",
    )
)

# Script generated for node machine_learning filter curated
SqlQuery243 = """
select DISTINCT * 
from step_trainer_trusted
INNER JOIN accelerometer_trusted on accelerometer_trusted.timestamp=step_trainer_trusted.sensorreadingtime;
"""
machine_learningfiltercurated_node1710258177452 = sparkSqlQuery(
    glueContext,
    query=SqlQuery243,
    mapping={
        "accelerometer_trusted": accelerometer_trustedS3source_node1710258081122,
        "step_trainer_trusted": step_trainer_trustedS3source_node1710258137086,
    },
    transformation_ctx="machine_learningfiltercurated_node1710258177452",
)

# Script generated for node machine_learning_curated S3 target
machine_learning_curatedS3target_node1710258370819 = glueContext.getSink(
    path="s3://jan-lake-house/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curatedS3target_node1710258370819",
)
machine_learning_curatedS3target_node1710258370819.setCatalogInfo(
    catalogDatabase="jan", catalogTableName="machine_learning_curated"
)
machine_learning_curatedS3target_node1710258370819.setFormat("json")
machine_learning_curatedS3target_node1710258370819.writeFrame(
    machine_learningfiltercurated_node1710258177452
)
job.commit()
