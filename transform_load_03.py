from generic_functions_01 import (
    db_connection,
    read_data_from_sql,
    write_data_to_sql,
    close_connection,
    clean_func,
    rename_column,
    raw_db,
    curated_db
)
from sqlalchemy import create_engine

# Read data from SQLite database
read_conn = db_connection(raw_db)
engine = create_engine(f"sqlite:///{curated_db}")

# Abilities
abilities_df = read_data_from_sql(read_conn, "abilities")
abilities_df = clean_func(
    abilities_df,
    null_check_cols=["standard_error", "lower_ci_bound"],
    replace_null_value=0
)
abilities_df = clean_func(
    abilities_df,
    null_check_cols=["upper_ci_bound"],
    replace_null_value=100
)
abilities_df = clean_func(
    abilities_df,
    null_check_cols=["not_relevant"],
    replace_null_value="Undefined"
)
write_data_to_sql(engine, abilities_df, "fact_abilities")

# Education_Training_Experience
education_training_experience_df = read_data_from_sql(
    read_conn, "education_training_experience"
)
education_training_experience_df = rename_column(
    education_training_experience_df, "n", "sample_size"
)
education_training_experience_df = clean_func(
    education_training_experience_df,
    null_check_cols=[
        "category", "data_value", "sample_size", "standard_error",
        "lower_ci_bound"
    ],
    replace_null_value=0
)

education_training_experience_df = clean_func(
    education_training_experience_df,
    null_check_cols=[
         "upper_ci_bound"
    ],
    replace_null_value=100
)
education_training_experience_df = clean_func(
    education_training_experience_df,
    null_check_cols=["recommend_suppress"],
    replace_null_value="Undefined"
)
write_data_to_sql(
    engine, education_training_experience_df, "fact_education_training_experience"
)

# Job_zone_reference
job_zone_reference_df = read_data_from_sql(read_conn, "job_zone_reference")
job_zone_reference_df = clean_func(
    job_zone_reference_df,
    null_check_cols=["name"],
    replace_null_value="Undefined"
)
write_data_to_sql(engine, job_zone_reference_df, "dim_job_zone_reference")

# Occupation Data
occupation_data_df = read_data_from_sql(read_conn, "occupation_data")
write_data_to_sql(engine, occupation_data_df, "dim_occupation_data")

# Occupation Level Metadata
occupation_level_metadata_df = read_data_from_sql(
    read_conn, "occupation_level_metadata"
)
occupation_level_metadata_df = rename_column(
    occupation_level_metadata_df, "n", "sample_size"
)
occupation_level_metadata_df = clean_func(
    occupation_level_metadata_df,
    null_check_cols=["response"],
    replace_null_value="No response"
)
occupation_level_metadata_df = clean_func(
    occupation_level_metadata_df,
    null_check_cols=["sample_size", "percent"],
    replace_null_value=0
)
write_data_to_sql(
    engine, occupation_level_metadata_df, "dim_occupation_level_metadata"
)

# Job zones
job_zones_df = read_data_from_sql(read_conn, "job_zones")
write_data_to_sql(engine, job_zones_df, "fact_job_zones")

# Knowledge
knowledge_df = read_data_from_sql(read_conn, "knowledge")
knowledge_df = rename_column(knowledge_df, "n", "sample_size")
knowledge_df = clean_func(
    knowledge_df,
    null_check_cols=[
        "data_value", "sample_size", "standard_error",
        "lower_ci_bound"
    ],
    replace_null_value=0
)
knowledge_df = clean_func(
    knowledge_df,
    null_check_cols=[
        "upper_ci_bound"
    ],
    replace_null_value=100
)

knowledge_df = clean_func(
    knowledge_df,
    null_check_cols=["recommend_suppress", "not_relevant"],
    replace_null_value="Undefined"
)
write_data_to_sql(engine, knowledge_df, "fact_knowledge")

# Skills
skills_df = read_data_from_sql(read_conn, "skills")
skills_df = rename_column(skills_df, "n", "sample_size")
skills_df = clean_func(
    skills_df,
    null_check_cols=[
        "data_value", "sample_size", "standard_error",
        "lower_ci_bound"
    ],
    replace_null_value=0
)
skills_df = clean_func(
    skills_df,
    null_check_cols=[
        "upper_ci_bound"
    ],
    replace_null_value=100
)
skills_df = clean_func(
    skills_df,
    null_check_cols=["recommend_suppress", "not_relevant"],
    replace_null_value="Undefined"
)
write_data_to_sql(engine, skills_df, "fact_skills")

# Level Scale Anchors
level_scale_df = read_data_from_sql(read_conn, "level_scale_anchors")
level_scale_df = clean_func(level_scale_df)
write_data_to_sql(engine, level_scale_df, "dim_level_scale_anchors")

# Close connection
close_connection(read_conn)