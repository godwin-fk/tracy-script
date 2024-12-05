import json
from uuid import uuid4

class Parser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def load_data(self):
        with open(self.file_path, 'r') as file:
            self.data = json.load(file)

    def parse_trigger(self):
        """Parse trigger-related data from the JSON."""
        error = self.data.get("error", {})
        trigger_exports = self.data.get("trigger", {}).get("exports", {})
        trigger_context = trigger_exports.get("context", {})

        email_event_data = (
            trigger_exports
            .get("event", {})
            .get("body", {})
            .get("email_generate_event_data", {})
            .get("$return_value", {})
        )
        # Always extract workflow related (id, trace_id, name, version) from trigger context
        # For trigger type, threadId, shipperId, agentId, extract from email_generate_event_data if present OTHERWISE from trigger context

        # For classifers we will have a step with naming pattern as {triggerType}_generate_event_data and props of this step will have shipper, agent details
        # EntityType and Id as null
        workflow_name = trigger_context.get("workflow_name", "")
        # if workflow name contains classifier, then extract shipper, agent details from the classifier step
        if "classifier" in workflow_name.lower():
            for step_name, step_data in self.data.get("steps", {}).items():
                if not isinstance(step_data, dict):
                    continue
                if "generate_event_data" in step_name:
                    trigger_exports = step_data.get("exports", {})
                    email_event_data = trigger_exports.get("$return_value", {})
                    break

        return {
            "workflow_id": trigger_context.get("workflow_id", ""),
            "workflow_name": trigger_context.get("workflow_name", ""),
            "platform_version": trigger_context.get("platform_version", ""),

            "shipper_id": email_event_data.get("shipper_id", ""),
            "agent_id": email_event_data.get("agent_id", ""),

            "trigger_type": email_event_data.get("type", ""),
            "thread_id": email_event_data.get("id", ""),
            "error": error
        }

    def parse_steps(self, trigger_data):
        """Parse the steps data from the JSON."""
        steps = self.data.get("steps", {})
        workflow_audit = []
        first_failure = "SUCCESS"
        first_failure_reason = ""
        final_comment = ""
        ignore_steps = ["store_workflow_steps_s3", "push-workflow-run-log"]
        for step_name, step_data in steps.items():
            if not isinstance(step_data, dict):
                continue
            if step_name in ignore_steps:
                continue

            step_error = step_data.get("error", {})
            status_reason = ""
            status = "FAILED" if step_error else "SUCCESS"
            if status == "FAILED":
                status_reason = step_error.get("message", "")
                # Set workflow status to FAILED if any step fails(first failure)
                if first_failure == "SUCCESS":
                    first_failure = status
                    first_failure_reason = status_reason

            timings = step_data.get("timings", {})
            summary = step_data.get("exports", {}).get("$summary", "") if step_data.get("exports") else ""
            final_comment = summary if summary else final_comment
            workflow_audit.append({
                "id": str(uuid4()),
                "thread_id": trigger_data["thread_id"], # same as trigger id at workflow run level

                "shipper_id": trigger_data["shipper_id"],
                "agent_id": trigger_data["agent_id"],
                "entity_type": None, # TODO: need to evaluate .. agent... tracy LOAD, SAM: ORDER
                "entity_id": None, # TODO: need to evaluate

                "workflow_id": trigger_data["workflow_id"],
                "step_id": step_data.get("id", ""),
                "step_name": step_name,

                "action": step_name,
                "status": status,
                "status_reason": status_reason,

                "comment": summary,
                "data": {}, # e.g. email data
                "action_timestamp": timings.get("end_ts", "") if timings else "",
                "idx": step_data.get("idx", 0)
            })

        # Sort the workflow_audit list by idx
        workflow_audit = sorted(workflow_audit, key=lambda x: x["idx"])

        return {
            # "first_failure": first_failure,
            "first_failure_reason": first_failure_reason,
            "final_comment": final_comment,
            "workflow_audit": workflow_audit
        }

    def parse_workflow(self):
        """Parse the entire workflow and return the results."""
        self.load_data()

        # Parse trigger
        trigger_data = self.parse_trigger()

        # Parse steps and audit
        step_data = self.parse_steps(trigger_data)
        workflow_audit = step_data["workflow_audit"]

        workflow_run = {
            "id": str(uuid4()),
            "exec_id": self.data.get("id"),
            # "trace_id": self.data.get("request_id"),
            "trace_id": trigger_data["thread_id"], 
            "parent_exec_id": "", # TODO: Get this for identifying child workflows

            "workflow_id": self.data.get("workflow_id"),
            "workflow_name": trigger_data["workflow_name"],
            "workflow_version": self.data.get("deployment_id"),
            "platform_version": trigger_data["platform_version"],

            "shipper_id": trigger_data.get("shipper_id"),
            "agent": trigger_data.get("agent_id"),

            "trigger_type": trigger_data["trigger_type"], # trigger, EMAIL, IM
            "trigger_id": trigger_data["thread_id"], # (not request_id from data itself) thread_id of the email or IM
            "trigger": "", # TODO: trigger context... eg. holdover_document, customer_query

            "entity_type": "", # TODO: need to evaluate .. agent... tracy LOAD, SAM: ORDER
            "entity_id": "", # TODO: need to evaluate
            "status": "FAILED" if trigger_data.get('error') or step_data["first_failure_reason"] else "SUCCESS", # Check if errors in any of the steps... FAILED otherwise SUCCESS .. (TODO: AWAITING, SKIPPED)
            "status_reason": trigger_data.get('error',{}).get('message',"") if trigger_data.get('error') or step_data["first_failure_reason"] else '', # error from the first step with error or workflow level error
            "comments": step_data["final_comment"], # summary of the most recent step
            "data": "", # TODO: {entity_related: {'scac': '', 'carrier_name': ''}} # would depend on entity_type


            # TODO: Derive from relevant action timestamp
            "followup_sent_at": "",
            "reminder_sent_at": "",
            "escalation_sent_at": "",
            "first_response_at": "", # TODO: timestamp of communication in/out pick from relevant step
            "latest_response_at": "", # TODO: timestamp of communication in/out pick from relevant step
        }

        return {
            "workflow_run": workflow_run,
            "workflow_audit": workflow_audit
        }
