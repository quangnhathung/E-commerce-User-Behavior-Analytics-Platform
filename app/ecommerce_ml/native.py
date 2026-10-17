"""Thin Spark 3.5 JVM bridge: built-in ML stages without Python NumPy/pandas extras.

Vector UDTs stay in the JVM. Only scalar feature columns and arrays cross into
Python, so SQL schema deserialization never imports pyspark.ml.linalg.
"""


class NativeML:
    def __init__(self, spark):
        self.spark = spark
        self.ml = spark._jvm.org.apache.spark.ml
        self.gateway = spark.sparkContext._gateway

    def array(self, kind, values):
        result = self.gateway.new_array(kind, len(values))
        for index, value in enumerate(values):
            result[index] = value
        return result

    def pipeline(self, stages):
        return self.ml.Pipeline().setStages(self.array(self.ml.PipelineStage, stages))

    def strings(self, values):
        return self.array(self.spark._jvm.java.lang.String, values)

    def transform(self, model, frame):
        from pyspark.sql import DataFrame
        return DataFrame(model.transform(frame._jdf), self.spark)

    def vector_array(self, frame, name="scaled_features"):
        from pyspark.sql.column import Column
        return Column(self.ml.functions.vector_to_array(frame._jdf.col(name), "float64"))

    def load(self, path):
        return self.ml.PipelineModel.load(path)
