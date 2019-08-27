COPY staging_world_happiness
FROM 's3://vijay-dend-capstone/world-happiness-report.json'
ACCESS_KEY_ID 'YOUR-KEY-ID'
SECRET_ACCESS_KEY 'YOUR-SECRET-ID'
FORMAT AS JSON 'auto';