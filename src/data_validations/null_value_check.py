from pyspark.sql.functions import col, trim, isnan, upper
from src.utility.report_lib import write_output
from pyspark.sql import SparkSession


def null_value_check(df, null_cols, num_records=5):
    """Validate that specified columns have no null or empty values."""

    failures = []

    for column in null_cols:   # ["customer_id","name", "email", "phone"]
        print("column", column)

        failing_rows = df.filter(
            f"""{column} is null 
                 or trim({column}) = '' 
                 or upper({column}) in ('NA','NULL','NONE')"""
        )

        null_count = failing_rows.count()
        print("null_count", null_count)

        if null_count > 0:
            # display null value rows on the console
            print(f"Null/blank/NA values found in column: {column}")
            failing_rows.show(num_records, truncate=False)

            failed_records = failing_rows.limit(num_records).collect()
            failed_preview = [row.asDict() for row in failed_records]

            failures.append({
                "column": column,
                "null_count": null_count,
                "sample_failed_records": failed_preview
            })

    if len(failures) > 0:
        status = "FAIL"
        write_output("Null Value Check", status, f"Failures: {failures}")
        return status
    else:
        status = "PASS"
        write_output("Null Value Check", status, "No null values found.")
        return status
