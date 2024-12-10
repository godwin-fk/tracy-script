CREATE TABLE "workflow_run" (
    "id" VARCHAR PRIMARY KEY,
    "trace_id" VARCHAR,
    "parent_id" VARCHAR,

    "workflow_id" VARCHAR,
    "workflow_version" VARCHAR,
    "platform_version" VARCHAR,

    "shipper_id" VARCHAR,
    "agent_id" VARCHAR,

    "trigger_type" VARCHAR,
    "trigger_id" VARCHAR,
    "trigger" VARCHAR,

    "entity_type" VARCHAR,
    "entity_id" VARCHAR,
    "status" VARCHAR,
    "status_reason" VARCHAR,
    "comments" VARCHAR,
    "data" JSONB,

    "followup_sent_at" TIMESTAMP WITH TIME ZONE,
    "reminder_sent_at"  TIMESTAMP WITH TIME ZONE,
    "escalation_sent_at"  TIMESTAMP WITH TIME ZONE,
    "first_response_at" TIMESTAMP WITH TIME ZONE,
    "latest_response_at" TIMESTAMP WITH TIME ZONE,

    "feedbacks" JSONB,
    "start_time" TIMESTAMP WITH TIME ZONE,
    "end_time" TIMESTAMP WITH TIME ZONE,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX idx_created_shipper_workflow ON "workflow_run" (created_at, shipper_id, workflow_id);

CREATE TABLE "workflow_audit" (
  "id" VARCHAR PRIMARY KEY,
  "request_id" VARCHAR,

  "shipper_id" VARCHAR,
  "agent_id" VARCHAR,
  "entity_type" VARCHAR,
  "entity_id" VARCHAR,

  "workflow_id" VARCHAR,
  "step_id" VARCHAR,
  "action" VARCHAR,
  "status" VARCHAR,
  "status_reason" VARCHAR,

  "comment" VARCHAR,
  "data" JSONB,

  "action_timestamp" TIMESTAMP WITH TIME ZONE,
  "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX idx_request_id ON "workflow_audit" ("request_id");
CREATE INDEX idx_timestamp_step ON "workflow_audit" ("action_timestamp", "step_id");
