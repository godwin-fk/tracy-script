# queries.py

query_db1 = '''
    SELECT
        t1.entity_id AS "load_id",
        t1.shipper_id AS "shipper_id", 
        t1.workflow_identifier AS "Workflow", 
        t1.status AS "Goal", 
        -- FILTER to get the first occurrence of EXTRACTED and first occurrence of UPDATED
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%EXTRACTED%') AS "start_time",
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%UPDATED%') AS "end_time",
        STRING_AGG(DISTINCT t1.action, '; ') AS "Actions"
    FROM 
        agentic_logger t1
    WHERE
        t1.entity_type = 'Load'
        AND DATE(t1.created_at) = CURRENT_DATE
    GROUP BY 
        t1.entity_id, t1.shipper_id, t1.workflow_identifier, t1.status;

'''

query_db2 = '''
    SELECT 
        t2.entity_id AS "load_id", 
        t2.status AS "status",
        STRING_AGG(t2.comments, '; ') AS "comments"
    FROM 
        milestones t2
    WHERE
        t2.entity_type = 'Load'
        AND DATE(t2.created_at) = CURRENT_DATE
    GROUP BY
        t2.entity_id, t2.status; 
'''
