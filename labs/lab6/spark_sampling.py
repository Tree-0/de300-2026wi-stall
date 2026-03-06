from pyspark.sql import SparkSession

print("Hello!")

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("randomSample") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    ### Read a CSV file stored in an Amazon S3 bucket
    print("Reading data")
    textFile = spark.read.csv("s3://stall-de300-winter26/lab6/data.csv", header = True)
    print("Read data!")
    
    ### Samples 1% of the data using Spark's sample() method
    samples = textFile.sample(.01, False, 42)
    print("Took sample!")
    
    ### Writes the sampled data back to S3 in Spark's distributed format
    samples.write.mode('overwrite').csv("s3://stall-de300-winter26/lab6/output/")
    print("Wrote all partitions to a directory")
    
    ### Converts the sample to Pandas and writes it as a traditional CSV file
    df = samples.toPandas()
    print("Made pandas")
    df.to_csv('s3://stall-de300-winter26/lab6/output.csv', index = False)
    print("Wrote file")

    spark.sparkContext.stop()
