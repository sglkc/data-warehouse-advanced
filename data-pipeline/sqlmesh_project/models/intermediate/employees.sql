MODEL (
    name intermediate.employees,
    kind INCREMENTAL_BY_UNIQUE_KEY(
        unique_key employee_id
    )
);

SELECT
    employee_id::INT AS employee_id,
    employee_name::TEXT AS employee_name,
    employee_email::TEXT AS employee_email,
    employee_phone::TEXT AS employee_phone,
    TO_DATE(employee_hire_date, 'YYYY/MM/DD') AS employee_hire_date,
    employee_job_title::TEXT AS employee_job_title,
    warehouse_id::INT AS warehouse_id,
    now(@var_timezone) AS load_ts /* Current timestamp in Jakarta timezone */
FROM staging.employees
