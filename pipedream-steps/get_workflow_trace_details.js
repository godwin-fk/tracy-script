const axios = require('axios');
require('dotenv').config();

class WorkflowStep {
    constructor(trace_id, api_token) {
        this.trace_id = trace_id;
        this.api_token = api_token;
    }

    async run({ $ }) {
        let config = {
            method: 'get',
            maxBodyLength: Infinity,
            url: `https://api.pipedream.com/graphql?operationName=executionTrace&variables={"id":"${this.trace_id}","includeTrigger":true,"namespaces":null}&extensions={"persistedQuery":{"sha256Hash":"95a4f663471f2e4664ca77cb3f5c48d9ef4a670d65d1544422e8d2fa63c2904d","version":1}}`,
            headers: {
              'Authorization': `Bearer ${this.api_token}`,
              'Cookie': 'pdsid=2068cbd637af72c29e9eedfaa99b3659'
            }
        };

        let response = await axios.request(config);
        return response.data.data.executionTrace.trace;
      }
  }

const workflowStep = new WorkflowStep('2pTNUlaQAMxT7bUffkgkhlydLSx', process.env.PIPEDREAM_API_TOKEN);
workflowStep.run({}).then((data) => {
    console.log(data);
}).catch((error) => {
    console.error(error);
});
