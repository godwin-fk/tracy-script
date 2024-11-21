
def get_agentic_audit_logs_query(workflow_identifier,shipper_id, start_date, end_date):
    query = f'''
            SELECT t1.entity_id AS "load_id",
                t1.request_id as "request_id",
                t1.shipper_id AS "shipper_id",
                t1.workflow_identifier AS "workflow",
				CASE
					WHEN t2.response_message IS NULL THEN 'AWAITING_RESPONSE'
					WHEN t2.response_updated_timestamp IS NULL THEN 'SKIPPED/INVALID_RESPONSE/FAILED'
					ELSE 'SUCCESS'
				END AS status,
                CASE
                    WHEN t1.comment like '%FourKites Alert: %' THEN t1.comment
                    ELSE NULL
                END AS "trigger_message",
                t2.response_message as "response_message",
                t1.created_at as "trigger_timestamp",
                t2.response_timestamp as "response_timestamp",
                t2.actions as "actions"
            from agentic_logger t1
            LEFT OUTER JOIN (SELECT entity_id,
                MAX(comment) FILTER (WHERE comment LIKE '%Raw message%') AS "response_message",
                MIN(created_at) FILTER (WHERE action = 'DETAILS_EXTRACTED' AND workflow_identifier IN ('shipment_update')) AS "response_timestamp",
		        MIN(created_at) FILTER (WHERE action LIKE '%_UPDATED%' AND workflow_identifier IN ('shipment_update')) AS "response_updated_timestamp",
                STRING_AGG(DISTINCT action, '; ') AS "actions"
            from agentic_logger
            WHERE (entity_type = 'Load' or entity_type = 'Shipment')
                AND request_id IN (select distinct request_id from agentic_logger where created_at >= '{start_date}' and created_at < '{end_date}' AND request_id is not null)
                AND shipper_id='{shipper_id}'
                AND workflow_identifier = 'shipment_update'
                GROUP BY request_id ,entity_id) as t2 ON (t1.entity_id = t2.entity_id AND t1.created_at < t2.response_time)
            WHERE (t1.entity_type = 'Load' or t1.entity_type = 'Shipment')
                AND t1.request_id IN (select distinct request_id from agentic_logger where created_at >= '{start_date}' and created_at < '{end_date}' AND request_id is not null)
                AND t1.shipper_id='{shipper_id}'
                AND t1.workflow_identifier = '{workflow_identifier}';
        '''

    return query


def get_milestones_query(shipper_id, load_ids):
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
                AND t2.entity_id in ({load_ids}.join(','))
                AND t2.shipper_id = '{shipper_id}'
            GROUP BY
                t2.entity_id, t2.status, t2.shipper_id;
        '''
    return query

