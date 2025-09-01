import polars as pl
from generic_functions_01 import db_connection, curated_db

# Connect to SQLite
conn = db_connection(curated_db)

# Helper to run query and return Polars DataFrame
def run_check(name: str, sql: str):
    df = pl.read_database(sql, conn)
    return name, df

# --- VALIDATION QUERIES ---

checks = [

    # 1. Missing occupation references in fact tables
    (
        "Missing occupation references in fact_skills",
        """
        SELECT DISTINCT f.onetsoc_code
        FROM fact_skills f
        LEFT JOIN dim_occupation_data d
          ON f.onetsoc_code = d.onetsoc_code
        WHERE d.onetsoc_code IS NULL;
        """
    ),
    (
        "Missing occupation references in fact_abilities",
        """
        SELECT DISTINCT f.onetsoc_code
        FROM fact_abilities f
        LEFT JOIN dim_occupation_data d
          ON f.onetsoc_code = d.onetsoc_code
        WHERE d.onetsoc_code IS NULL;
        """
    ),

    # 2. Job zones out of expected range
    (
        "Invalid job_zone values",
        """
        SELECT *
        FROM fact_job_zones
        WHERE job_zone NOT BETWEEN 1 AND 5;
        """
    ),

    # 3. Invalid confidence intervals in knowledge
    (
        "Invalid CI bounds in fact_knowledge",
        """
        SELECT onetsoc_code, element_id, scale_id, data_value, lower_ci_bound, upper_ci_bound
        FROM fact_knowledge
        WHERE data_value < lower_ci_bound
           OR data_value > upper_ci_bound;
        """
    ),

    # 4. Duplicate rows in fact_skills
    (
        "Duplicate rows in fact_skills",
        """
        SELECT onetsoc_code, element_id, scale_id, COUNT(*) AS dup_count
        FROM fact_skills
        GROUP BY onetsoc_code, element_id, scale_id
        HAVING COUNT(*) > 1;
        """
    ),

    # 5. Occupations missing any skill/knowledge/ability record
    (
        "Occupations with no skills",
        """
        SELECT d.onetsoc_code, d.title, fs.onetsoc_code as fact_skills_onetsoc_code
        FROM dim_occupation_data d
        LEFT JOIN fact_skills fs
          ON d.onetsoc_code = fs.onetsoc_code
        WHERE fs.onetsoc_code IS NULL;
        """
    ),
    (
        "Occupations with no abilities",
        """
        SELECT d.onetsoc_code, d.title, fa.onetsoc_code as fact_abilities_onetsoc_code
        FROM dim_occupation_data d
        LEFT JOIN fact_abilities fa
          ON d.onetsoc_code = fa.onetsoc_code
        WHERE fa.onetsoc_code IS NULL;
        """
    )
]

# --- RUN ALL CHECKS ---
results = []
for name, sql in checks:
    check_name, df = run_check(name, sql)
    results.append((check_name, df))

# --- DISPLAY RESULTS ---
for name, df in results:
    print(f"\n=== {name} ===")
    if df.is_empty():
        print("✅ No issues found")
    else:
        print(df)
