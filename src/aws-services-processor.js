const _ = require('lodash');
const AWS = require('aws-sdk');
const StateMachineError = require('./state-machine-error');

// The following aws services:
// - arn:aws:states:::sqs:sendMessage

class AWSServicesProcessor {
    constructor(config) {
        this.config = config

        const options = {
            credentials: (this.config.accessKeyId && this.config.secretAccessKey) ? {accessKeyId: this.config.accessKeyId, secretAccessKey: this.config.secretAccessKey} : undefined,
            region: this.config.region ? this.config.region : 'eu-west-1'
        }

        AWS.config.update(options)
    }

    processAWSServices(stateInfo, input) {
        switch(stateInfo.Resource) {
            case 'arn:aws:states:::sqs:sendMessage':
                return this.sqsSendMessage(stateInfo, input)
            default:
                throw new StateMachineError('This AWS Service isn\'t implemented');
        }
    }
        
    async sqsSendMessage(stateInfo, input) {
        const sqs = new AWS.SQS()
        let queueUrl = input.QueueUrl

        if (this.config.sqs && this.config.sqs.endpoint) {
            const queueInfo = queueUrl.split('/')
            queueUrl = this.config.sqs.endpoint + queueInfo[queueInfo.length - 1]
        }

        let params = {
            MessageBody: JSON.stringify(input.MessageBody),
            QueueUrl: queueUrl
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

module.exports = AWSServicesProcessor;
