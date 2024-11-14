# queries.py

# t1.shipper_id = 'smithfield-foods' AS "shipper_id", 
# CASE 
#     WHEN t1.comment LIKE '%Raw message%' THEN t1.comment
#     ELSE NULL 
# END AS "Comments",
# MIN(CASE WHEN t1.comment LIKE '%Raw message%' THEN t1.created_at END) AS "Comments",
def get_agentic_audit_logs_query(workflow_identifier,shipper_id, start_date, end_date):
    query = f'''    
        SELECT  t1.entity_id AS "load_id",
                t1.request_id as "request_id",
                t1.shipper_id AS "shipper_id",
                t1.workflow_identifier AS "workflow",
                t1.status AS "goal", 		
                t2.comment as "raw_message",
                t1.comment as "alert",
                t1.created_at as "start_time",
                t2.created_at as "end_time",
                t2.actions as "actions"
        from agentic_logger t1 
        LEFT OUTER JOIN (SELECT entity_id,
                MAX(comment) FILTER (WHERE comment LIKE '%Raw message%') AS "comment",
                MIN(created_at) FILTER (WHERE action LIKE '%UPDATED%' AND workflow_identifier IN ('shipment_update','report_based_shipment_update')) AS "created_at",
                STRING_AGG(DISTINCT action, '; ') AS "actions"
        from agentic_logger
        WHERE (entity_type = 'Load' or entity_type = 'Shipment')
                AND request_id IN (select distinct request_id from agentic_logger where DATE(created_at) >= '{start_date}' and DATE(created_at) <= '{end_date}') 
                AND shipper_id='{shipper_id}'
                AND workflow_identifier = 'shipment_update'
                GROUP BY request_id ,entity_id) as t2 ON (t1.entity_id = t2.entity_id AND t1.created_at < t2.created_at)
        WHERE (t1.entity_type = 'Load' or t1.entity_type = 'Shipment')
                AND t1.request_id IN (select distinct request_id from agentic_logger where DATE(created_at) >= '{start_date}' and DATE(created_at) <= '{end_date}') 
                AND t1.shipper_id='{shipper_id}'
                AND t1.workflow_identifier = '{workflow_identifier}';

        '''
    return query


def get_milestones_query(shipper_id, start_date, end_date):
    query = f'''
    SELECT 
        t2.entity_id AS "load_id", 
        t2.shipper_id,
        t2.status AS "status",
        STRING_AGG(t2.comments, '; ') AS "comments"
    FROM 
        milestones t2
    WHERE
        t2.entity_type = 'Load'
        AND t2.entity_id in (select distinct entity_id from milestones where DATE(created_at) >= '{start_date}' and DATE(created_at) <= '{end_date}')
        AND t2.shipper_id='{shipper_id}'
    GROUP BY
        t2.entity_id, t2.status,t2.shipper_id; 
'''
    return query


