import sqlite3
from pathlib import Path
from typing import Optional, Any
import polars as pl

# Base directory for SQL scripts
BASE_SQL_DIR = Path.cwd() / "sql_scripts"
raw_db = 'raw_occupation.db'
curated_db = 'curated_occupation.db'
# -----------------------------
# File & SQL Script Utilities
# -----------------------------
def read_sql_script(filename: str) -> Optional[str]:
    """
    Reads a SQL script file from the 'sql_scripts' subdirectory and returns its contents as a string.

    Parameters
    ----------
    filename : str
        Name of the SQL file to read (e.g., 'init_schema.sql').

    Returns
    -------
    str or None
        The full SQL script as a string if the file is successfully read; otherwise, None.
    """

    sql_path = BASE_SQL_DIR / filename
    print(f"Opening file: {sql_path}")

    try:
        return sql_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"Could not open file with error: {e}")
        return None


# -----------------------------
# DataFrame Cleaning Utilities
# -----------------------------
def standardize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts all column names in a Polars DataFrame to snake_case format.

    This includes:
    - Stripping leading and trailing whitespace
    - Converting all characters to lowercase
    - Replacing spaces with underscores

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame whose column names will be standardized.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with updated column names in snake_case format.
    """

    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df


def trim_whitespace(df: pl.DataFrame) -> pl.DataFrame:
    """
    Removes leading and trailing whitespace from all string columns in a Polars DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame whose string columns will be cleaned.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with whitespace trimmed from all string-type columns.
        Non-string columns are returned unchanged.
    """
    return df.select([
        pl.col(col).str.strip_chars() if df.schema[col] == pl.String else pl.col(col)
        for col in df.columns
    ])


def fill_nulls(df: pl.DataFrame, cols: list[str], default: Any) -> pl.DataFrame:
    """
    Fills null values in specified columns of a Polars DataFrame with a default value.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame containing potential null values.
    cols : list[str]
        A list of column names to apply the null-filling operation to.
    default : Any
        The value to use for replacing nulls in the specified columns.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with nulls replaced in the specified columns. If a column
        in `cols` does not exist in the DataFrame, it will be added with all values set to `None`.
    """

    return df.with_columns([
        pl.col(col).fill_null(default) if col in df.columns else pl.lit(None).alias(col)
        for col in cols
    ])


def rename_column(df: pl.DataFrame, old_name: str, new_name: str) -> pl.DataFrame:
    """
    Renames a column in a Polars DataFrame.

    Parameters:
    - df (pl.DataFrame): The input DataFrame.
    - old_name (str): The current name of the column to rename.
    - new_name (str): The new name for the column.

    Returns:
    - pl.DataFrame: A new DataFrame with the column renamed.
    """

    if old_name not in df.columns:
        raise ValueError(f"Column '{old_name}' does not exist in the DataFrame.")
    return df.rename({old_name: new_name})


def clean_func(
    df: pl.DataFrame,
    null_check_cols: Optional[list[str]] = None,
    replace_null_value: Optional[Any] = None
) -> pl.DataFrame:
    """
    Cleans the given DataFrame by optionally checking for nulls in specified columns
    and replacing them with a given value.

    Parameters
    ----------
    df : pl.DataFrame
        The input DataFrame.
    null_check_cols : Optional[list[str]], default None
        List of column names to check for null values. If None, no null check is performed.
    replace_null_value : Optional[object], default None
        Value to replace nulls with. If None, nulls are not replaced.

    Returns
    -------
    pl.DataFrame
        The cleaned DataFrame.
    """
    
    df = trim_whitespace(standardize_column_names(df))

    if null_check_cols and replace_null_value is not None:
        df = fill_nulls(df, null_check_cols, replace_null_value)

    return df


# -----------------------------
# Database Utilities
# -----------------------------
def db_connection(db_name: str) -> sqlite3.Connection:
    """
    Establishes a connection to a SQLite database.

    Parameters
    ----------
    db_name : str
        The name or path of the SQLite database file.

    Returns
    -------
    sqlite3.Connection
        A connection object to interact with the specified SQLite database.
    """

    return sqlite3.connect(db_name)


def commit_transaction(conn: sqlite3.Connection) -> None:
    """
    Commits the current transaction on the given database connection.

    Parameters
    ----------
    conn : sqlite3.Connection
        An active SQLite database connection object.

    Returns
    -------
    None
    """

    conn.commit()


def create_cursor(conn: sqlite3.Connection) -> sqlite3.Cursor:
    """
    Creates a new cursor object using the given database connection.

    Parameters
    ----------
    conn : sqlite3.Connection
        An active SQLite database connection object.

    Returns
    -------
    sqlite3.Cursor
        A new cursor object for executing SQL commands.
    """

    return conn.cursor()


def close_connection(conn: sqlite3.Connection) -> None:
    """
    Closes an active database connection.

    Parameters
    ----------
    conn : sqlite3.Connection or compatible DB-API connection
        The database connection object to be closed.

    Returns
    -------
    None
    """

    conn.close()


# -----------------------------
# SQL Read/Write
# -----------------------------
def read_data_from_sql(conn: sqlite3.Connection, table_name: str) -> pl.DataFrame:
    """
    Reads all data from a specified SQL table and returns it as a Polars DataFrame.

    Parameters:
    ----------
    conn : sqlite3.Connection or compatible DB-API connection
        An active database connection object.
    table_name : str
        The name of the SQL table to read data from.

    Returns:
    -------
    pl.DataFrame
        A Polars DataFrame containing all rows and columns from the specified table.
    """

    cursor = create_cursor(conn)
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    return pl.DataFrame(rows, schema=col_names, orient="row")


def write_data_to_sql(engine: Any, df: pl.DataFrame, table_name: str) -> Optional[str]:
    """
    Writes a Polars DataFrame to a specified SQL table.

    Parameters
    ----------
    df : pl.DataFrame
        The Polars DataFrame to write to the SQL table.
    conn : sqlite3.Connection or compatible DB-API connection
        An active database connection object.
    table_name : str
        The name of the SQL table to write data to.

    Returns
    -------
    None
    """

    try:
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists="replace"  # Options: 'replace', 'append', 'fail'
        )
        return f"Write successful for table: {table_name}"
    except Exception as e:
        print(f"Error writing DataFrame to SQL table '{table_name}': {e}")
        return None