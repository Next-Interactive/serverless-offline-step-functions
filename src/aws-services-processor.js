const _ = require('lodash');
const AWS = require('aws-sdk');

// const StateMachineError = require('./state-machine-error');

// The following aws services:
// - arn:aws:states:::sqs:sendMessage

class AWSServicesProcessor {
    processAWSServices(stateInfo, input) {
        switch(stateInfo.Resource) {
            case 'arn:aws:states:::sqs:sendMessage':
                return this.sendMessage(stateInfo, input)
        }
    }
        
    async sendMessage(stateInfo, input) {
        const sqs = new AWS.SQS({
            'region': 'eu-west-1'
        })
        
        let params = {
            MessageBody: JSON.stringify(input.MessageBody),
            QueueUrl: input.QueueUrl
        };

        if (input.DelaySeconds !== undefined) {
            params.DelaySeconds = input.DelaySeconds
        }

        if (input.MessageDeduplicationId !== undefined) {
            params.MessageDeduplicationId = input.MessageDeduplicationId
        }

        if (input.MessageGroupId !== undefined) {
            params.MessageGroupId = input.MessageGroupId
        }

        if (input.MessageAttributes !== undefined) {
            params.MessageAttributes = input.MessageAttributes
        }

        return await sqs.sendMessage(params).promise()
    }
}

module.exports = new AWSServicesProcessor();
