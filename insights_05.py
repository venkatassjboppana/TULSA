from generic_functions_01 import db_connection, curated_db
import polars as pl


def run_query(conn, title: str, sql: str) -> None:
    """
    Executes a SQL query, prints a heading, and displays the result as a Polars DataFrame.

    Parameters
    ----------
    conn : sqlite3.Connection
        Active database connection.
    title : str
        Heading to display before the query results.
    sql : str
        SQL query string to execute.
    """
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")
    df = pl.read_database(sql, conn)
    print(df)


if __name__ == "__main__":
    # Connect to SQLite
    conn = db_connection(curated_db)

    # 1️⃣ Top 10 Skills for High-Preparation Jobs
    sql_top_skills = """
    SELECT 
        dlsa.anchor_description AS skill_name,
        ROUND(AVG(fs.data_value), 2) AS avg_skill_score
    FROM fact_skills fs
    JOIN fact_job_zones fjz 
        ON fs.onetsoc_code = fjz.onetsoc_code
    JOIN dim_job_zone_reference djzr 
        ON fjz.job_zone = djzr.job_zone
    JOIN dim_level_scale_anchors dlsa 
        ON fs.element_id = dlsa.element_id
       AND fs.scale_id = dlsa.scale_id
    WHERE djzr.job_zone >= 4
    GROUP BY dlsa.anchor_description
    ORDER BY avg_skill_score DESC
    LIMIT 10;
    """
    run_query(conn, "Top 10 Skills for High-Preparation Jobs", sql_top_skills)

    # 2️⃣ Average Knowledge Score by Job Zone
    sql_avg_knowledge = """
    SELECT 
        djzr.job_zone,
        djzr.name AS job_zone_name,
        ROUND(AVG(fk.data_value), 2) AS avg_knowledge_score
    FROM fact_knowledge fk
    JOIN fact_job_zones fjz 
        ON fk.onetsoc_code = fjz.onetsoc_code
    JOIN dim_job_zone_reference djzr 
        ON fjz.job_zone = djzr.job_zone
    GROUP BY djzr.job_zone, djzr.name
    ORDER BY djzr.job_zone;
    """
    run_query(conn, "Average Knowledge Score by Job Zone", sql_avg_knowledge)

    # 3️⃣ Occupations with Highest Ability Requirements
    sql_highest_abilities = """
    SELECT 
        dod.title AS occupation_title,
        dlsa.anchor_description AS ability_name,
        ROUND(AVG(fa.data_value), 2) AS avg_ability_score
    FROM fact_abilities fa
    JOIN dim_occupation_data dod 
        ON fa.onetsoc_code = dod.onetsoc_code
    JOIN dim_level_scale_anchors dlsa 
        ON fa.element_id = dlsa.element_id
       AND fa.scale_id = dlsa.scale_id
    GROUP BY dod.title, dlsa.anchor_description
    ORDER BY avg_ability_score DESC
    LIMIT 10;
    """
    run_query(conn, "Occupations with Highest Ability Requirements", sql_highest_abilities)

    # 4️⃣ Occupations with Broadest Ability Requirements
    sql_broadest_abilities = """
    SELECT 
        dod.title AS occupation_title,
        COUNT(DISTINCT fa.element_id) AS distinct_abilities_count
    FROM fact_abilities fa
    JOIN dim_occupation_data dod 
        ON fa.onetsoc_code = dod.onetsoc_code
    GROUP BY dod.title
    ORDER BY distinct_abilities_count DESC
    LIMIT 10;
    """
    run_query(conn, "Occupations with Broadest Ability Requirements", sql_broadest_abilities)

    conn.close()
