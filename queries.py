# queries.py

# t1.shipper_id = 'smithfield-foods' AS "shipper_id", 
# CASE 
#     WHEN t1.comment LIKE '%Raw message%' THEN t1.comment
#     ELSE NULL 
# END AS "Comments",
# MIN(CASE WHEN t1.comment LIKE '%Raw message%' THEN t1.created_at END) AS "Comments",
query_db1 = '''
    SELECT
        t1.entity_id AS "load_id",
        'smithfield-foods' AS "shipper_id",
        t1.workflow_identifier AS "Workflow",
        t1.status AS "Goal", 
        MAX(t1.comment) FILTER (WHERE t1.comment LIKE '%Raw message%') AS "Raw Response",
        MAX(t1.comment) FILTER (WHERE t1.comment LIKE '%FourKites Alert%') AS "Alert",
        -- FILTER to get the first occurrence of EXTRACTED and first occurrence of UPDATED
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%EXTRACTED%') AS "start_time",
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%UPDATED%') AS "end_time",
        STRING_AGG(DISTINCT t1.action, '; ') AS "Actions"
    FROM 
        agentic_logger t1
    WHERE
        (t1.entity_type = 'Load' or t1.entity_type = 'Shipment')
        AND DATE(t1.created_at) >= '2024-10-28' and DATE(t1.created_at) <= '2024-11-08'
    GROUP BY 
        t1.entity_id, t1.shipper_id, t1.workflow_identifier, t1.status;

'''

query_db2 = '''
    SELECT 
        t2.entity_id AS "load_id", 
        'smithfield-foods' AS "shipper_id",
        t2.status AS "status",
        STRING_AGG(t2.comments, '; ') AS "comments"
    FROM 
        milestones t2
    WHERE
        t2.entity_type = 'Load'
        AND DATE(t2.created_at) >= '2024-10-28' and DATE(t2.created_at) <= '2024-11-08'
    GROUP BY
        t2.entity_id, t2.status; 
'''
