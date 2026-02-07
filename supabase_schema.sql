-- schema.sql (Postgres / Supabase)

CREATE TABLE dams (
    dam_id VARCHAR(20) PRIMARY KEY,
    dam_name VARCHAR(255) NOT NULL,
    full_volume INT,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6)
);

CREATE TABLE latest_data (
    dam_id VARCHAR(20) PRIMARY KEY,
    dam_name VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    storage_volume DECIMAL(10, 3),
    percentage_full DECIMAL(6, 2),
    storage_inflow DECIMAL(10, 3),
    storage_release DECIMAL(10, 3),
    FOREIGN KEY (dam_id) REFERENCES dams(dam_id)
);

CREATE TABLE dam_resources (
    resource_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dam_id VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    storage_volume DECIMAL(10, 3),
    percentage_full DECIMAL(6, 2),
    storage_inflow DECIMAL(10, 3),
    storage_release DECIMAL(10, 3),
    FOREIGN KEY (dam_id) REFERENCES dams(dam_id)
);

CREATE TABLE specific_dam_analysis (
    dam_id VARCHAR(20),
    analysis_date DATE,
    avg_storage_volume_12_months DECIMAL(10, 3),
    avg_storage_volume_5_years DECIMAL(10, 3),
    avg_storage_volume_10_years DECIMAL(10, 3),
    avg_percentage_full_12_months DECIMAL(6, 2),
    avg_percentage_full_5_years DECIMAL(6, 2),
    avg_percentage_full_10_years DECIMAL(6, 2),
    avg_storage_inflow_12_months DECIMAL(10, 3),
    avg_storage_inflow_5_years DECIMAL(10, 3),
    avg_storage_inflow_10_years DECIMAL(10, 3),
    avg_storage_release_12_months DECIMAL(10, 3),
    avg_storage_release_5_years DECIMAL(10, 3),
    avg_storage_release_10_years DECIMAL(10, 3),
    PRIMARY KEY (dam_id, analysis_date),
    FOREIGN KEY (dam_id) REFERENCES dams(dam_id)
);

CREATE TABLE overall_dam_analysis (
    analysis_date DATE PRIMARY KEY,
    avg_storage_volume_12_months DECIMAL(10, 3),
    avg_storage_volume_5_years DECIMAL(10, 3),
    avg_storage_volume_10_years DECIMAL(10, 3),
    avg_percentage_full_12_months DECIMAL(6, 2),
    avg_percentage_full_5_years DECIMAL(6, 2),
    avg_percentage_full_10_years DECIMAL(6, 2),
    avg_storage_inflow_12_months DECIMAL(10, 3),
    avg_storage_inflow_5_years DECIMAL(10, 3),
    avg_storage_inflow_10_years DECIMAL(10, 3),
    avg_storage_release_12_months DECIMAL(10, 3),
    avg_storage_release_5_years DECIMAL(10, 3),
    avg_storage_release_10_years DECIMAL(10, 3)
);

CREATE TABLE dam_groups (
    group_name VARCHAR(255) PRIMARY KEY
);

CREATE TABLE dam_group_members (
    group_name VARCHAR(255) NOT NULL,
    dam_id VARCHAR(20) NOT NULL,
    PRIMARY KEY (group_name, dam_id),
    FOREIGN KEY (group_name) REFERENCES dam_groups(group_name),
    FOREIGN KEY (dam_id) REFERENCES dams(dam_id)
);