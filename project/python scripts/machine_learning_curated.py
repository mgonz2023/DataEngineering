import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1678230844169 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://marcos-data-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1678230844169",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://marcos-data-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Landing
CustomerLanding_node1678230902852 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://marcos-data-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1678230902852",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1678231033651 = Filter.apply(
    frame=CustomerLanding_node1678230902852,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="CustomerTrusted_node1678231033651",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1678231099792 = Join.apply(
    frame1=CustomerTrusted_node1678231033651,
    frame2=AccelerometerLanding_node1678230844169,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="AccelerometerTrusted_node1678231099792",
)

# Script generated for node Step Trainer Join Accelerometer Data
StepTrainerJoinAccelerometerData_node1678231157653 = Join.apply(
    frame1=AccelerometerTrusted_node1678231099792,
    frame2=StepTrainerLanding_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="StepTrainerJoinAccelerometerData_node1678231157653",
)

# Script generated for node Aggregate
Aggregate_node1678231232547 = sparkAggregate(
    glueContext,
    parentFrame=StepTrainerJoinAccelerometerData_node1678231157653,
    groups=[],
    aggs=[["timeStamp", "avg"]],
    transformation_ctx="Aggregate_node1678231232547",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Aggregate_node1678231232547,
    mappings=[("`avg(timeStamp)`", "double", "`avg(timeStamp)`", "double")],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://marcos-data-lake-house/step_trainer/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()
