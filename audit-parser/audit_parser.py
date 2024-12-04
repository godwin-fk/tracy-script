import json

def parse(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)

        trigger_context = data.get("trigger", {}).get("exports", {}).get("context", {})
        workflow_run = {
            "id": data.get("id"), # Generate UUID
            "exec_id": data.get("id"),
            "trace_id": data.get("request_id"),
            "parent_exec_id": "", # TODO: Get this for identifying child workflows

            "workflow_id": data.get("workflow_id"),
            "workflow_name": trigger_context.get("workflow_name"),
            "workflow_version": data.get("deployment_id"),
            "platform_version": trigger_context.get("platform_version"),

            "shipper_id": "", # TODO: trigger context
            "agent": "", # TODO: trigger context

            "trigger_type": "", # trigger, EMAIL, IM
            "trigger_id": "", # thread_id of the email or IM
            "trigger": "", # TODO: trigger context... eg. holdover_document, customer_query

            "entity_type": "", # TODO: need to evaluate .. agent... tracy LOAD, SAM: ORDER
            "entity_id": "", # TODO: need to evaluate
            "status": "", # Check if errors in any of the steps... FAILURE otherwise SUCCESS .. (TODO: AWAITING, SKIPPED)
            "status_reason": "", # error from the first step with error or workflow level error
            "comments": "", # summary of the most recent step
            "data": "", # TODO: {entity_related: {'scac': '', 'carrier_name': ''}} # would depend on entity_type

            # TODO: Derive from relevant action timestamp
            "enquiry_sent_at": "",
            "reminder_sent_at": "",
            "escalation_sent_at": "",
            "first_communication_time": "", # TODO: timestamp of communication in/out pick from relevant step
            "last_communication_time": "", # TODO: timestamp of communication in/out pick from relevant step
        }

        steps = data.get("steps", {})
        workflow_audit = []
        for step_name, step_data in steps.items(): # order by idx asc
            if isinstance(step_data, dict):
                print(f"Step: {step_name}")
                step_info = steps.get(step_name, {})
                step_id =  step_info.get("id",{})
                start_time = step_info.get("timings", {}).get("start_ts")
                end_time = step_info.get("timings", {}).get("end_ts")
                # if audit present in return_value use that as it is. TODO: some transformation if needed
                if step_name == "email_generate_event_data": 
                    event_data = step_info['exports']['$return_value']
                else:
                    event_data = step_info.get("props", {}).get("event_data",{})

                audit_entry = {
                    "id": "", # Generate UUID
                    "thread_id": "", # same as trigger id at workflow run level

                    "shipper_id": "", # should come from workflow level
                    "agent": "", # should come from workflow level
                    "entity_type": "", # TODO: need to evaluate .. agent... tracy LOAD, SAM: ORDER
                    "entity_id": "", # TODO: need to evaluate

                    "workflow_id": "", # should come from workflow level,
                    "step_id": step_id,
                    "step_name": step_name,

                    "action": "", # What was done in this step... e.g. email_generate_event_data, email_send, email_receive, email_parse, email_extract, email_validate, email_process, email_update, email_notify, email_alert
                    "status": "", # if no errors then success else failure..
                    "status_reason": "", # error from the step

                    "comment": "", # summary of the step
                    "data": "", # e.g. email data

                    "start_time": start_time,
                    "end_time": end_time,

                    # "parent_request_id": event_data.get("parent_request_id")
                }
                workflow_audit.append(audit_entry)
        return {
            "workflow_run": workflow_run,
            "workflow_audit": workflow_audit
        }

json_file_path = "logs/ready-to-pickup.json"
parsed_output = parse(json_file_path)

print(json.dumps(parsed_output, indent=4))