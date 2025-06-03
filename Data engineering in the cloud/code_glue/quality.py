import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.functions import col, to_date, date_format, to_timestamp, udf, StringType, dayofmonth, month, year, size, array_contains, explode, when, regexp_extract
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import IntegerType, DateType, DoubleType, DecimalType
from pyspark.sql.functions import mean as _mean, percentile_approx


# Parse command-line arguments
args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                            'INPUT_BUCKET',
                            'OUTPUT_BUCKET'])

# Create a SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def convert_date_columns(df, columns_to_cast):
    
    for col_name in columns_to_cast:
        df = df.withColumn(col_name, substring(df[col_name], 1, 10))
        df = df.withColumn(col_name, df[f"{col_name}"].cast(DateType()))
    return df


def convert_numbers_columns(df, columns_to_cast):
    
    for col_name in columns_to_cast:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    return df


def drop_columns(df, columns_to_drop):
    return df.drop(*columns_to_drop)


def validate_and_write_parquet(df, database_name, table_name, output_s3_path, spark):
    # Check if the db exists
    try:
        spark.sql(f"USE {database_name}")
    except:
        print(f"Database {database_name} does not exist. Creating the database.")
        spark.sql(f"CREATE DATABASE {database_name}")
        
        
    # Check if the table exists
    try:
        spark.table(table_name)
        print(f"Table {table_name} exists. Overwriting the table.")
        # If table exists, overwrite it
        df.write.option('path',output_s3_path).format("parquet").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

    except AnalysisException:
        print(f"Table {table_name} does not exist. Creating the table.")
        df.write.option('path',output_s3_path).format("parquet").saveAsTable(f"{database_name}.{table_name}")


def get_part_date(df, column_date):
    for column in column_date:
        day_col = f"day_{column}"
        month_col = f"month_{column}"
        year_col = f"year_{column}"
        df = df.withColumn(day_col, dayofmonth(df[column])) \
            .withColumn(month_col, month(df[column])) \
            .withColumn(year_col, year(df[column]))
        # Always drop the original date column
        df = df.drop(column)
        # Drop any new part column that is all null
        for part_col in [day_col, month_col, year_col]:
            if df.filter(col(part_col).isNotNull()).count() == 0:
                df = df.drop(part_col)
    return df


def sample_and_replace_id(df, sample_size=1000, id_col='id'):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    # Take a random sample
    df_sampled = df.orderBy(F.rand()).limit(sample_size)
    # Generate new IDs: CL001 to CL1000
    new_ids = [f"CL{str(i+1).zfill(3)}" for i in range(sample_size)]
    # Create a DataFrame with new IDs
    id_df = spark.createDataFrame([(i,) for i in new_ids], [id_col])
    # Add a row number to join
    df_sampled = df_sampled.withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    id_df = id_df.withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    # Join and replace id
    df_final = df_sampled.drop(id_col).join(id_df, on="row_num").drop("row_num")
    return df_final


def one_hot_encode_columns(df, columns):
    """
    Performs one-hot encoding for the specified categorical columns in a Spark DataFrame.
    Each unique value in a column becomes a new column with 1/0 values.
    """
    for col_name in columns:
        unique_values = df.select(col_name).distinct().rdd.flatMap(lambda x: x).collect()
        for val in unique_values:
            # Normalize value for column name: lowercase, remove accents, replace spaces with underscores
            import unicodedata
            safe_val = str(val).lower()
            safe_val = ''.join(c for c in unicodedata.normalize('NFD', safe_val) if unicodedata.category(c) != 'Mn')
            safe_val = safe_val.replace(' ', '_').replace('-', '_').replace('/', '_')
            df = df.withColumn(f"{col_name}_{safe_val}", when(col(col_name) == val, 1).otherwise(0))
        df = df.drop(col_name)
    return df


def convert_term_to_months(df, col_name='term'):
    """
    Converts a string column like '36 months' to an integer column with just the number of months.
    The new column is named 'term_in_months'. The original column is dropped.
    """
    df = df.withColumn('term_in_months', regexp_extract(col(col_name), r'(\\d+)', 1).cast('int'))
    df = df.drop(col_name)
    return df


def fillna_with_mode(df, columns):
    """
    Fill NA values in the specified columns with the mode (most frequent value) for each column.
    """
    from pyspark.sql import functions as F
    for col_name in columns:
        mode_row = df.groupBy(col_name).count().orderBy(F.desc('count')).first()
        if mode_row is not None:
            mode_value = mode_row[0]
            df = df.fillna({col_name: mode_value})
    return df


def normalize_string_column(df, col_name):
    """
    Normalize a string column: lowercase, remove accents, replace spaces with underscores.
    """
    import unicodedata
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def normalize(s):
        if s is None:
            return None
        s = s.lower()
        s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        s = s.replace(' ', '_')
        return s
    normalize_udf = udf(normalize, StringType())
    return df.withColumn(col_name, normalize_udf(col(col_name)))


def clean_percentage_column(df, col_name):
    """
    Removes the '%' character from a string column and casts it to double.
    """
    from pyspark.sql.functions import regexp_replace
    df = df.withColumn(col_name, regexp_replace(col(col_name), '%', ''))
    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    return df


def aggregate_industrial_by_cliente_id(df):
    """
    Aggregates the Industrial dataset by cliente_id, averaging saldo_servicio and all numeric columns.
    Returns one row per cliente_id.
    """
    from pyspark.sql import functions as F
    numeric_cols = [c for c, t in df.dtypes if t in ['double', 'float', 'int', 'bigint'] and c != 'cliente_id']
    agg_exprs = [F.avg(c).alias(c) for c in numeric_cols]
    # For one-hot columns, take max (since they are 0/1)
    one_hot_cols = [c for c in df.columns if any([c.startswith(p) for p in ['servicios_contratados_', 'estado_servicio_']])]
    agg_exprs += [F.max(c).alias(c) for c in one_hot_cols]
    # For all other columns, take first (e.g., date parts, categorical)
    other_cols = [c for c in df.columns if c not in numeric_cols + one_hot_cols + ['cliente_id']]
    agg_exprs += [F.first(c).alias(c) for c in other_cols]
    
    # Validate column name for groupBy
    group_col = None
    for c in df.columns:
        if c.lower().replace(' ', '_') == 'cliente_id':
            group_col = c
            break
    if not group_col:
        raise ValueError("No column matching 'cliente_id' found for groupBy operation.")
    return df.groupBy(group_col).agg(*agg_exprs)


def main():
    # CONTOSO
    df_input_contoso = f"{args['INPUT_BUCKET']}Contoso/"
    df_contoso_read = spark.read.parquet(df_input_contoso)
    contoso_columns = [col for col in df_contoso_read.columns if col == 'id'] + [col for col in df_contoso_read.columns if col != 'id']
    contoso_columns = contoso_columns[:17] if 'id' in contoso_columns[:17] else ['id'] + contoso_columns[1:17]
    df_contoso = df_contoso_read.select(*contoso_columns)
    df_contoso = sample_and_replace_id(df_contoso, sample_size=1000, id_col='id')
    columns_to_drop = ['member_id', 'funded_amnt', 'funded_amnt_inv', 'grade', 'subgrade', 'verification_status', 'emp_length', 'home_ownership']
    columns_to_cast_number = ['loan_amnt', 'installment', 'annual_inc']
    columns_to_cast_date = ['issue_d']
    columns_to_get_part_date = ['issue_d']
    df_contoso = drop_columns(df_contoso, columns_to_drop)
    df_contoso = clean_percentage_column(df_contoso, 'int_rate')
    df_contoso = convert_numbers_columns(df_contoso, columns_to_cast_number)
    df_contoso = convert_date_columns(df_contoso, columns_to_cast_date)
    df_contoso = get_part_date(df_contoso, columns_to_get_part_date)
    df_contoso = convert_term_to_months(df_contoso, 'term')

    # COLOMBIA
    df_input_col = f"{args['INPUT_BUCKET']}Colombia/"
    df_col_read = spark.read.parquet(df_input_col)
    columns_to_drop_col = ['pais', 'codigo_postal']
    columns_to_cast_number_col = ['antiguedad_empleo_meses', 'hijos', 'vehiculos']
    df_col = drop_columns(df_col_read, columns_to_drop_col)
    df_col = convert_numbers_columns(df_col, columns_to_cast_number_col)
    df_col = fillna_with_mode(df_col, ['ciudad', 'empleador', 'propiedad_vivienda'])
    df_col = one_hot_encode_columns(df_col, ['ciudad', 'empleador', 'propiedad_vivienda'])

    # PERABANK
    df_input_perabank = f"{args['INPUT_BUCKET']}Perabank/"
    df_perabank_read = spark.read.parquet(df_input_perabank)
    df_perabank = df_perabank_read.withColumn("num_servicios_contratados", size(col("servicios_contratados")))
    df_perabank = drop_columns(df_perabank, ["servicios_contratados", 'email'])
    columns_to_cast_number_perabank = ['dispositivos_activos', 'frecuencia_login_mensual']
    df_perabank = convert_numbers_columns(df_perabank, columns_to_cast_number_perabank)
    columns_to_cast_date_perabank = ['ultima_actividad']
    columns_to_get_part_date_perabank = ['ultima_actividad']
    df_perabank = convert_date_columns(df_perabank, columns_to_cast_date_perabank)
    df_perabank = get_part_date(df_perabank, columns_to_get_part_date_perabank)
    df_perabank = one_hot_encode_columns(df_perabank, ['preferencia_canal'])

    # INDUSTRIAL BANK (join result from RDS PostgreSQL)
    df_input_industrial = f"{args['INPUT_BUCKET']}IndustrialBank/"
    df_industrial_read = spark.read.parquet(df_input_industrial)
    df_industrial = df_industrial_read
    # Normalize column names to ASCII (remove accents, lower, replace spaces and special characters)
    import unicodedata
    import re
    def normalize_colname(name):
        name = name.lower()
        name = ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')
        name = re.sub(r'[^a-z0-9_]', '_', name)  # Replace any non-alphanumeric/underscore with _
        name = re.sub(r'_+', '_', name)  # Collapse multiple underscores
        name = name.strip('_')
        return name
    col_map = {c: normalize_colname(c) for c in df_industrial.columns}
    # Print mapping for debug
    print('IndustrialBank column normalization mapping:')
    for orig, norm in col_map.items():
        print(f"  '{orig}' -> '{norm}'")
    # Rename columns in DataFrame
    for orig, norm in col_map.items():
        if orig != norm and norm not in df_industrial.columns:
            df_industrial = df_industrial.withColumnRenamed(orig, norm)
    # Remove extra underscores from column names (e.g., 'a_n_n_u_a_l_i_n_c' -> 'annual_inc')
    # Remove extra underscores ONLY for columns where every character is separated by a single underscore (e.g., a_n_n_u_a_l_i_n_c)
    def remove_extra_underscores(name):
        # Match pattern: single lowercase letter followed by _ repeatedly, e.g., a_n_n_u_a_l_i_n_c
        parts = name.split('_')
        if len(parts) > 1 and all(len(p) == 1 for p in parts):
            return ''.join(parts)
        return name
    # Build a new mapping for columns with extra underscores
    col_map_clean = {}
    for c in df_industrial.columns:
        cleaned = remove_extra_underscores(c)
        if cleaned != c and cleaned not in df_industrial.columns:
            col_map_clean[c] = cleaned
    # Print mapping for debug
    if col_map_clean:
        print('Removing extra underscores from columns:')
        for orig, cleaned in col_map_clean.items():
            print(f"  '{orig}' -> '{cleaned}'")
    # Rename columns in DataFrame
    for orig, cleaned in col_map_clean.items():
        df_industrial = df_industrial.withColumnRenamed(orig, cleaned)

    # --- Ensure all key columns have correct snake_case names after normalization ---
    # Map of expected column names (snake_case) to possible collapsed forms
    key_columns = [
        'servicios_contratados', 'cliente_id', 'annual_inc', 'score_crediticio',
        'producto_id', 'saldo_servicio', 'fecha_apertura_servicio', 'estado_servicio'
    ]
    for expected in key_columns:
        collapsed = expected.replace('_', '')
        # If collapsed version exists and expected does not, rename it
        if collapsed in df_industrial.columns and expected not in df_industrial.columns:
            print(f"Renaming column '{collapsed}' to '{expected}' (snake_case fix)")
            df_industrial = df_industrial.withColumnRenamed(collapsed, expected)

    # --- DEBUG: Confirm presence and name of all key columns after normalization ---
    for expected in key_columns:
        found_col = None
        for c in df_industrial.columns:
            if normalize_colname(c) == expected:
                found_col = c
                break
        if found_col:
            print(f"Column '{expected}' found as: '{found_col}' after normalization.")
        else:
            print(f"Warning: No column matching '{expected}' found after normalization!")

    # Now reference normalized column names
    columns_to_drop_industrial = ['nombre', 'apellido', 'ciudad', 'producto_id']
    df_industrial = drop_columns(df_industrial, columns_to_drop_industrial)
    # Dynamically find normalized 'fecha_apertura_servicio' column
    fecha_col = None
    for c in df_industrial.columns:
        if normalize_colname(c) == 'fecha_apertura_servicio':
            fecha_col = c
            break
    if fecha_col:
        df_industrial = convert_date_columns(df_industrial, [fecha_col])
        df_industrial = get_part_date(df_industrial, [fecha_col])
    else:
        print("Warning: No column matching 'fecha_apertura_servicio' found after normalization.")
    columns_to_cast_number_industrial = ['annual_inc', 'score_crediticio', 'saldo_servicio']
    columns_to_cast_number_industrial = [c for c in columns_to_cast_number_industrial if c in df_industrial.columns]
    df_industrial = convert_numbers_columns(df_industrial, columns_to_cast_number_industrial)
    df_industrial = normalize_string_column(df_industrial, 'servicios_contratados')
    df_industrial = one_hot_encode_columns(df_industrial, ['servicios_contratados', 'estado_servicio'])
    # Aggregate by cliente_id to ensure one row per client
    df_industrial = aggregate_industrial_by_cliente_id(df_industrial)

    # --- Merge step ---
    df_merged = df_col.join(df_perabank, on='cliente_id', how='left')
    df_merged = df_merged.join(df_contoso, df_merged['cliente_id'] == df_contoso['id'], how='left')
    # --- Ensure unique and valid column names in both dataframes before unionByName ---
    import re
    key_columns_set = set([
        'servicios_contratados', 'cliente_id', 'annual_inc', 'score_crediticio',
        'producto_id', 'saldo_servicio', 'fecha_apertura_servicio', 'estado_servicio'
    ])
    one_hot_prefixes = [
        'ciudad_', 'empleador_', 'propiedad_vivienda_', 'preferencia_canal_',
        'servicios_contratados_', 'estado_servicio_'
    ]
    def is_one_hot(col_name):
        return any([col_name.startswith(p) for p in one_hot_prefixes])
    def clean_column_names(columns):
        seen = set()
        cleaned_cols = []
        for col_name in columns:
            # For key columns and one-hot columns, only remove null bytes/non-printable
            if col_name in key_columns_set or is_one_hot(col_name):
                clean = re.sub(r'[\x00-\x1F\x7F]', '', col_name)
            else:
                # Remove null bytes and special characters, collapse underscores
                clean = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
                clean = re.sub(r'_+', '_', clean)
                clean = clean.strip('_')
            # Ensure uniqueness
            orig_clean = clean
            i = 1
            while clean in seen:
                clean = f"{orig_clean}_{i}"
                i += 1
            seen.add(clean)
            cleaned_cols.append(clean)
        return cleaned_cols

    # Clean columns for both dataframes
    df_merged = df_merged.toDF(*clean_column_names(df_merged.columns))
    df_industrial = df_industrial.toDF(*clean_column_names(df_industrial.columns))
    df_final = df_merged.unionByName(df_industrial, allowMissingColumns=True)

    # Now drop only 'id' if present, but keep 'cliente_id'
    columns_to_drop = []
    if 'id' in df_final.columns:
        columns_to_drop.append('id')
    if columns_to_drop:
        df_final = df_final.drop(*columns_to_drop)

    # Identify column types
    one_hot_prefixes = [
        'ciudad_', 'empleador_', 'propiedad_vivienda_', 'preferencia_canal_',
        'servicios_contratados_', 'estado_servicio_'
    ]
    one_hot_cols = [c for c in df_final.columns if any([c.startswith(p) for p in one_hot_prefixes])]
    # Date part columns (all columns starting with 'day_', 'month_', 'year_')
    date_part_cols = [c for c in df_final.columns if c.startswith('day_') or c.startswith('month_') or c.startswith('year_')]
    # Numeric columns (double/float/int, excluding one-hot and date parts)
    numeric_cols = [c for c, t in df_final.dtypes if t in ['double', 'float', 'int', 'bigint'] and c not in one_hot_cols and c not in date_part_cols]
    # Categorical columns (string, not one-hot, not date parts, not id)
    categorical_cols = [c for c, t in df_final.dtypes if t == 'string' and c not in one_hot_cols and c not in date_part_cols and c not in ['cliente_id']]

    # Fill one-hot encoded columns with 0
    for col_name in one_hot_cols:
        df_final = df_final.fillna({col_name: 0})

    # Fill numeric columns with mean
    for col_name in numeric_cols:
        if col_name in df_final.columns:
            mean_val = df_final.select(_mean(col_name)).first()[0]
            if mean_val is not None:
                df_final = df_final.fillna({col_name: mean_val})

    # Fill categorical columns with mode
    for col_name in categorical_cols:
        if col_name in df_final.columns:
            mode_row = df_final.groupBy(col_name).count().orderBy('count', ascending=False).first()
            if mode_row is not None and mode_row[0] is not None:
                mode_val = mode_row[0]
                df_final = df_final.fillna({col_name: mode_val})

    # Fill date part columns with mode
    for col_name in date_part_cols:
        if col_name in df_final.columns:
            mode_row = df_final.groupBy(col_name).count().orderBy('count', ascending=False).first()
            if mode_row is not None and mode_row[0] is not None:
                mode_val = mode_row[0]
                df_final = df_final.fillna({col_name: mode_val})

    # Shuffle and drop duplicates before writing
    from pyspark.sql import functions as F
    df_final = df_final.orderBy(F.rand())
    df_final = df_final.dropDuplicates()

    validate_and_write_parquet(df_final, "database_merged", "table_merged", f"{args['OUTPUT_BUCKET']}Merged/", spark)

if __name__ == "__main__":
    main()