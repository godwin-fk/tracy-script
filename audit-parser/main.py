import json

def parse_workflow_and_steps(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Extract workflow-level details from trigger.context
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

    # Extract steps from all root-level keys containing $return_value
    audit_trail = []
    for step_name, step_data in data.items():
        if isinstance(step_data, dict) and "$return_value" in step_data:
            step_info = step_data.get("$return_value", {})
            audit_entry = {
                "step_id": step_info.get("step_id", step_name),
                "step_name": step_name,
                "start_time": step_info.get("start_time"),
                "end_time": step_info.get("end_time"),
                "status": "success" if step_info.get("status") == "completed" else "failure",
                "data": step_info  # Store the entire data block as JSON
            }
            audit_trail.append(audit_entry)

    return {
        "workflow_context": workflow_info,
        "audit_trail": audit_trail
    }

# Path to the JSON file
json_file_path = "ready-to-pickup-audit.json"

# Parse the JSON file
parsed_output = parse_workflow_and_steps(json_file_path)

# Pretty-print the results
print(json.dumps(parsed_output, indent=4))
