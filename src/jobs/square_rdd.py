def run(spark, **kwargs):
    """Run the square rdd job."""
    data = range(11)
    rdd = spark.sparkContext.parallelize(data)
    squares = rdd.map(lambda x: x**2)
    result = squares.collect()
    print(result)
