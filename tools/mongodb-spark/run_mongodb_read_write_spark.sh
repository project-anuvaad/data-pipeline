spark-submit --master "local[4]"  \
        --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/anuvaad.testpeople?readPreference=primaryPreferred" \
        --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/anuvaad.testpeople" \
        --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 \
        mongodb_read_write_spark.py
