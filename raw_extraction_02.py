from generic_functions_01 import (
    commit_transaction,
    db_connection,
    read_sql_script,
    create_cursor,
    close_connection,
    raw_db
)


def execute_sql_scripts(db_name: str, sql_files: list[str]) -> None:
    """
    Executes a list of SQL script files against a SQLite database.

    Parameters
    ----------
    db_name : str
        Path to the SQLite database file.
    sql_files : list[str]
        List of SQL filenames to execute.
    """
    conn = db_connection(db_name)
    cursor = create_cursor(conn)

    for file_name in sql_files:
        sql_script = read_sql_script(file_name)

        if not sql_script:
            print(f"No SQL script returned for file: {file_name}")
            continue

        try:
            cursor.executescript(sql_script)
            print(f"Executed SQL script from file: {file_name}")
        except Exception as e:
            print(f"Error executing SQL script from file: {file_name}, Error: {e}")

    commit_transaction(conn)
    close_connection(conn)


if __name__ == "__main__":
    SQL_FILES = [
        '02_job_zone_reference.sql', '03_occupation_data.sql',
        '06_level_scale_anchors.sql', '07_occupation_level_metadata.sql',
        '11_abilities.sql', '12_education_training_experience.sql',
        '14_job_zones.sql', '15_knowledge.sql', '16_skills.sql'
    ]

    execute_sql_scripts(raw_db, SQL_FILES)
