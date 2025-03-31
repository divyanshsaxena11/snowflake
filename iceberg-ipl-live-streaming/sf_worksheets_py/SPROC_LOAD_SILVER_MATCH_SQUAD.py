# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp


def main(session: snowpark.Session):
    # Your code goes here, inside the "main" handler.
    matches_data_df = session.sql(
        "SELECT *, CURRENT_TIMESTAMP AS LOAD_DATETIME FROM CRICKETINFO_DB.IN_IPL.MATCH_SQUAD"
    )
    matches_data_df.write.mode("append").save_as_table(
        "CDW_LAKEHOUSE.SILVER.MATCH_SQUAD"
    )
    # Return value will appear in the Results tab.
    return matches_data_df
