import json
def parse(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
        
        trigger_context = data.get("trigger", {}).get("exports", {}).get("context", {}) 
        workflow_info = {
            "id": data.get("id"),
            "trace_id": data.get("request_id"),
            "workflow_id": data.get("workflow_id"),
            "workflow_name": trigger_context.get("workflow_name"),
            "workflow_deployment_id": data.get("deployment_id"),
            "start_time": data.get("execution_start_ts"),
            "end_time": data.get("execution_end_ts"),
            "version": trigger_context.get("platform_version"),
            # "total_time": trigger_context.get("total_time"),
            # "compute_time": trigger_context.get("compute_time"),
            # "credits": trigger_context.get("credits"),
        }
        
        steps = data.get("steps", {})
        audit_trail = []
        for step_name, step_data in steps.items():
            if isinstance(step_data, dict):
                print(f"Step: {step_name}")
                step_info = steps.get(step_name, {})
                step_id =  step_info.get("id",{})
                start_time = step_info.get("timings", {}).get("start_ts")
                end_time = step_info.get("timings", {}).get("end_ts")
                if step_name == "email_generate_event_data": 
                    event_data = step_info['exports']['$return_value']
                else:
                    event_data = step_info.get("props", {}).get("event_data",{})
                    audit_entry = {
                        "step_name": step_name,
                        "step_id": step_id,
                        "id": event_data.get("id"),
                        "agent_id": event_data.get("agent_id"),
                        "shipper_id": event_data.get("shipper_id"),
                        "workflow_id": event_data.get("workflow_id"),
                        "parent_request_id": event_data.get("parent_request_id"),
                        "start_time": start_time,
                        "end_time": end_time,
                        # "status": "success" if step_info.get("status") == "completed" else "failure",
                        # "data": step_info  # enabling this clutters the output
                    }
                    audit_trail.append(audit_entry)
        return {
            "workflow_context": workflow_info,
            "audit_trail": audit_trail
        }

json_file_path = "logs/ready-to-pickup.json"
parsed_output = parse(json_file_path)

print(json.dumps(parsed_output, indent=4))