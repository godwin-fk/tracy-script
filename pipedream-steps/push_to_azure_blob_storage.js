const { BlobServiceClient } = require("@azure/storage-blob");
require("dotenv").config();

class WorkflowStep {
  constructor(connectionString, containerName, data) {
    this.connectionString = connectionString;
    this.containerName = containerName;
    this.data = data;
  }

  async run({ $ }) {
    const context = this.data.trigger.exports.context;
    const date = new Date(context.ts);
    const year = date.getUTCFullYear(); // Get year
    const month = String(date.getUTCMonth() + 1).padStart(2, "0"); // Get month (0-based index)
    const day = String(date.getUTCDate()).padStart(2, "0"); // Get day
    const hour = String(date.getUTCHours()).padStart(2, "0"); // Get hour

    // trace/v1/<workspace ID>/<workflow ID>/YYYY/MM/DD/HH/<trace id>/<run id>.json
    const blobName = `trace/v1/${context.owner_id}/${context.workflow_id}/${year}/${month}/${day}/${hour}/${context.trace_id}/${context.id}.txt`;

    const blobServiceClient = BlobServiceClient.fromConnectionString(
      this.connectionString
    );
    const containerClient = blobServiceClient.getContainerClient(
      this.containerName
    );
    const blockBlobClient = containerClient.getBlockBlobClient(this.blobName);
    const jsonData = JSON.stringify(this.data);
    const uploadResponse = await blockBlobClient.upload(
      jsonData,
      jsonData.length
    );

    console.log(
      `Blob "${this.blobName}" uploaded successfully to container "${this.containerName}".`
    );
    console.log("Upload details:", this.uploadResponse);
  }
}

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const containerName = "workflow-run-logs";
const data = {
  key: "value",
  name: "example",
  trigger: {
    exports: {
      context: {
        id: "2pzQFYv0Eh8RkOX5iEuMeBtQcDT",
        ts: "2024-12-09T17:45:20.563Z",
        pipeline_id: null,
        workflow_id: "p_pWCxQWl",
        deployment_id: "d_L7s28xzJ",
        source_type: "TRACE",
        verified: false,
        hops: null,
        test: false,
        replay: true,
        owner_id: "o_rAI1rz5",
        platform_version: "3.52.0",
        workflow_name: "not-test-worflow",
        resume: null,
        emitter_id: "dc_ZduD7jX",
        external_user_id: null,
        external_user_environment: null,
        trace_id: "2pzQFYv0Eh8RkOX5iEuMeBtQcDT",
        project_id: "proj_PNsAdKM",
      },
    },
  },
};

const workflowStep = new WorkflowStep(connectionString, containerName, data);
workflowStep
  .run({})
  .then((data) => {
    console.log(data);
  })
  .catch((error) => {
    console.error(error);
  });
