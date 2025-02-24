"""This module contains the ratings counter job."""

import collections


def run(spark, **kwargs):
    """Run the ratings counter job."""
    # lines = spark.sparkContext.textFile("data/ml-100k/u.data")
    df = spark.read.csv(
        "data/ml-100k/u.data", sep="\t", header=False, inferSchema=True
    ).toDF("userid", "itemid", "rating", "timestamp")
    # lines = df.rdd.map(lambda r: r[0])
    # ratings = lines.map(lambda x: x.split()[2])
    # result = ratings.countByValue()
    result = df.groupBy("rating").count().orderBy("rating")
    result.show()

    # result = {row['rating']: row['count'] for row in result}

    # sorted_result = collections.OrderedDict(sorted(result.items()))
    # for key, value in sorted_result.items():
    #     print(f"{key} {value}")
