import json
def parse(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
        
        trigger_context = data.get("trigger", {}).get("context", {})
        workflow_info = {
            "workflow_exec_id": trigger_context.get("workflow_exec_id"),
            "workflow_id": trigger_context.get("workflow_id"),
            "workflow_name": trigger_context.get("workflow_name"),
            "start_time": trigger_context.get("start_time"),
            "end_time": trigger_context.get("end_time"),
            "version": trigger_context.get("version"),
            "total_time": trigger_context.get("total_time"),
            "compute_time": trigger_context.get("compute_time"),
            "credits": trigger_context.get("credits"),
        }
        
        steps = data.get("steps", {})
        audit_trail = []
        for step_name, step_data in steps.items():
            if isinstance(step_data, dict) and step_name == "email_generate_event_data": 
                step_info = steps.get(step_name, {})
                event_data = step_info['exports']['$return_value']
                audit_entry = {
                    "step_id": event_data.get("id"),
                    "step_name": step_name,
                    "agent_id": event_data.get("agent_id"),
                    "shipper_id": event_data.get("shipper_id"),
                    "workflow_id": event_data.get("workflow_id"),
                    "parent_request_id": event_data.get("parent_request_id"),
                    # "start_time": step_info.get("start_time"),
                    # "end_time": step_info.get("end_time"),
                    # "status": "success" if step_info.get("status") == "completed" else "failure",
                    # "data": step_info  # Store the entire data block as JSON
                }
                audit_trail.append(audit_entry)
            else:
                print(f"Step: {step_name} does not contain $return_value")
        return {
            "workflow_context": workflow_info,
            "audit_trail": audit_trail
        }

json_file_path = "logs/ready-to-pickup.json"
parsed_output = parse(json_file_path)

print(json.dumps(parsed_output, indent=4))