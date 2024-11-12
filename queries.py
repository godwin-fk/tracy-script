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
        t1.shipper_id AS "shipper_id",
        t1.workflow_identifier AS "Workflow",
        t1.status AS "Goal", 
        MAX(t1.comment) FILTER (WHERE t1.comment LIKE '%Raw message%') AS "Raw Response",
        MAX(t1.comment) FILTER (WHERE t1.comment LIKE '%FourKites Alert%') AS "Alert",
        -- FILTER to get the first occurrence of EXTRACTED and first occurrence of UPDATED
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%EXTRACTED%'or t1.action = 'FETCHED_CARRIER_SCAC') AS "start_time",
        MIN(t1.created_at) FILTER (WHERE t1.action LIKE '%UPDATED%' AND t1.workflow_identifier IN ('shipment_update','report_based_shipment_update')) AS "end_time",
        STRING_AGG(DISTINCT t1.action, '; ') AS "Actions"
    FROM 
        agentic_logger t1
    WHERE
        (t1.entity_type = 'Load' or t1.entity_type = 'Shipment')
        AND t1.request_id IN (select distinct request_id from agentic_logger where DATE(created_at) >= '2024-10-31' and DATE(created_at) <= '2024-11-06') 
        AND t1.shipper_id='smithfield-foods'
    GROUP BY 
        t1.entity_id, t1.shipper_id, t1.workflow_identifier, t1.status;

'''

query_db2 = '''
    SELECT 
        t2.entity_id AS "load_id", 
        t2.shipper_id,
        t2.status AS "status",
        STRING_AGG(t2.comments, '; ') AS "comments"
    FROM 
        milestones t2
    WHERE
        t2.entity_type = 'Load'
        AND t2.entity_id in (select distinct entity_id from milestones where DATE(created_at) >= '2024-10-31' and DATE(created_at) <= '2024-11-06')
        AND t2.shipper_id='smithfield-foods'
    GROUP BY
        t2.entity_id, t2.status,t2.shipper_id; 
'''
