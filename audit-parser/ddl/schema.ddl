CREATE TABLE "workflow_run" (
    "id" VARCHAR PRIMARY KEY,
    "exec_id" VARCHAR,
    "trace_id" VARCHAR,

    "workflow_id" VARCHAR,
    "workflow_version" VARCHAR,
    "platform_version" VARCHAR,

    "shipper_id" VARCHAR,
    "agent" VARCHAR,

    "trigger_type" VARCHAR,
    "trigger_id" VARCHAR,
    "trigger" VARCHAR,

    "entity_type" VARCHAR,
    "entity_id" VARCHAR,
    "status" VARCHAR,
    "resolution_status" VARCHAR,
    "comments" VARCHAR,
    "data" JSONB,

    "enquiry_sent_at" TIMESTAMP WITH TIME ZONE,
    "reminder_sent_at"  TIMESTAMP WITH TIME ZONE,
    "escalation_sent_at"  TIMESTAMP WITH TIME ZONE,

    "last_communication_time" BIGINT,
    "feedbacks" JSONB,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
    "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX idx_created_shipper_workflow ON "workflow_run" (created_at, shipper_id, workflow_id);

CREATE TABLE "workflow_audit" (
  "id" VARCHAR PRIMARY KEY,
  "thread_id" VARCHAR,

  "shipper_id" VARCHAR,
  "agent" VARCHAR,
  "entity_type" VARCHAR,
  "entity_id" VARCHAR,

  "workflow_id" VARCHAR,
  "action_id" VARCHAR,
  "action" VARCHAR,
  "status" VARCHAR,
  "status_reason" VARCHAR,

  "comment" VARCHAR,
  "data" JSONB,

  "action_timestamp" TIMESTAMP WITH TIME ZONE,
  "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE INDEX idx_thread_id ON "workflow_audit" ("thread_id");
CREATE INDEX idx_timestamp_action ON "workflow_audit" ("action_timestamp", "action_id");

CREATE TABLE "workflow_enquiry" (
  "thread_id" VARCHAR,
  "workflow_run_id" VARCHAR
);

CREATE INDEX idx_thread_id_workflow_run_id ON "workflow_enquiry" ("thread_id", "workflow_run_id");
