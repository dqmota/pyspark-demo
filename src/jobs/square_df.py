import pandas as pd


def run(spark, **kwargs):
    """Run the square rdd job."""

    # data = pd.DataFrame({
    #     'num': [num for num in range(11)]
    # })

    # df = spark.createDataFrame(data)
    df = spark.range(0, 10).toDF("num")
    squares = df.withColumn("square", df.num**2)
    result = squares.show()
    print(result)
