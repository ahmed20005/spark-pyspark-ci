from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean input DataFrame:
    - Remove rows where amount <= 0
    - Remove rows where name is NULL
    - Add column amount_with_tax = amount * 1.20
    """
    return (
        df.filter(col("amount") > 0)
          .filter(col("name").isNotNull())
          .withColumn("amount_with_tax", col("amount") * 1.20)
    )
