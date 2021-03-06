'use strict';

const _ = require('lodash');
const path = require('path');
const Promise = require('bluebird');
const fs = require('fs');
const stateTypes = require('./src/state-types');
const functionHelper = require('serverless-offline/src/functionHelper');

class ServerlessPlugin {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.options = options;
    this.logPrefix = '[Offline Step Functions] ';
    this.handlersDirectory = `./node_modules/serverless-offline-step-functions/src`;
    this.awsServices = ['arn:aws:states:::sqs:sendMessage']
    this.hooks = {
        'before:offline:start:init': () => require('./src/step-functions-api-simulator.js')(this.serverless, this.awsServices),
        'offline:start:init': () =>
            Promise.bind(this)
            .then(this.parseYaml)
            // TODO: validate state names
            // State machine, execution, and activity names must be 1–80 characters in length,
            // must be unique for your account and region, and must not contain any of the following:
            // - Whitespace
            // - Wildcard characters (? *)
            // - Bracket characters (< > { } [ ])
            // - Special characters (: ; , \ | ^ ~ $ # % & ` ")
            // - Control characters (\\u0000 - \\u001f or \\u007f - \\u009f).
            .then(this.createEndpoints)
            .then(this.createStepFunctionsJSON),
    };
  }

  /**
   * For each statemachine, set up the appropriate endpoint resource to kick off
   * the execution of the state machine and generate a handler file for the endpoint
   * A custom request template is used to send the state machine name and starting
   * state's name to the lambda's execution
   */
  createEndpoints() {
    // object key in the service's custom data for config values
    const SERVERLESS_OFFLINE_STEP_FUNCTIONS = 'serverless-offline-step-functions';
    const functions = this.serverless.service.functions;
    
    _.forEach(this.serverless.service.stepFunctions.stateMachines, (stateMachine, stateMachineName) => {
        _.forEach(stateMachine.definition.States, (state, stateName) => {
            if (state.Type === stateTypes.TASK && !this.awsServices.includes(state.Resource)) {
                let lambdaName = this.serverless.providers.aws.naming.extractLambdaNameFromArn(state.Resource);

                // store the lambda function handler in the state for reference in the JSON file
                // it will be used to call the proper handler code when executing the fucntion
                // as part of the state machine
                if (this.serverless.service.custom &&
                    this.serverless.service.custom[SERVERLESS_OFFLINE_STEP_FUNCTIONS] &&
                    this.serverless.service.custom[SERVERLESS_OFFLINE_STEP_FUNCTIONS].resourcePrefix) {
                        const regex = new RegExp(this.serverless.service.custom[SERVERLESS_OFFLINE_STEP_FUNCTIONS].resourcePrefix);
                        lambdaName = lambdaName.replace(regex, '');
                }

                if (!functions[lambdaName]) {
                    throw new Error(`Lambda function not found: ${lambdaName}`);
                }

                state.handler = functions[lambdaName].handler;
                state.environment = functions[lambdaName].environment;
                
                if (stateName === stateMachine.definition.StartAt) {
                    const lambdaFn = this.service.getFunction(lambdaName);
                    // // create a new function for an endpoint and
                    // // give it a unique name
                    // const lambdaFn = {};
                    // const functionName = `${lambdaName}-StepFunction${Date.now()}`;

                    // give the new function the same events as it's state machine twin
                    lambdaFn.events = Object.assign([], stateMachine.events);

                    // set the handler to the generic state machine handler function
                    lambdaFn.startHandler = `${this.handlersDirectory}/state-machine-handler.run`;
                    _.forEach(lambdaFn.events, (event) => {
                        if (event.http) {
                            event.input = { stateName: stateMachine.definition.StartAt, stateMachine: stateMachineName };
                            event.http.integration = 'lambda';
                            event.http.request = {
                                headers: {
                                    'Content-type': 'application/json'
                                },
                                // this custom template mimics input to StepFunctions
                                // and sends the state name and state machine name
                                template: {
                                    'application/json':
                                    require('./src/state-machine-request-template')(stateMachineName, stateMachine.definition.StartAt),
                                },
                            };

                            // needed to include response headers otherwise sls offline threw an error:
                            // TypeError: Uncaught error: Cannot read property 'headers' of undefined
                            event.http.response = {
                                headers: {
                                    'Content-type': 'application/json',
                                },
                            };
                        }
                    });

                    // add to serverless functions
                    functions[lambdaName] = lambdaFn;
                    this.serverless.cli.log(`${this.logPrefix} created ${lambdaName}`);
                }
            }
        });
    });


  }

  /**
   * Creates a JSON file for reference during the state machine execution
   */
  createStepFunctionsJSON() {
    fs.writeFileSync(`${this.handlersDirectory}/step-functions.json`, JSON.stringify(this.serverless.service.stepFunctions));
  }

  /**
   * Adds the step function configuration to the serverless config
   * @author serverless-step-functions
   */
  parseYaml() {
    this.serverless.config.servicePath = this.serverless.config.servicePath.replace(".build", "");
    const servicePath = this.serverless.config.servicePath;
    if (!servicePath) {
        console.error(this.logPrefix, 'servicePath not found');
        return Promise.resolve();
    }

    const serverlessYmlPath = path.join(servicePath, 'serverless.yml');

    return this.serverless.yamlParser
    .parse(serverlessYmlPath)
    .then(serverlessFileParam => {
        this.serverless.service.stepFunctions = {};
        this.serverless.service.stepFunctions.stateMachines
            = serverlessFileParam.stepFunctions
        && serverlessFileParam.stepFunctions.stateMachines
            ? serverlessFileParam.stepFunctions.stateMachines : {};
        this.serverless.service.stepFunctions.activities
            = serverlessFileParam.stepFunctions
        && serverlessFileParam.stepFunctions.activities
            ? serverlessFileParam.stepFunctions.activities : [];

        if (!this.serverless.pluginManager.cliOptions.stage) {
            this.serverless.pluginManager.cliOptions.stage = this.options.stage ||
            (this.serverless.service.provider && this.serverless.service.provider.stage) ||
            'dev';
        }

        if (!this.serverless.pluginManager.cliOptions.region) {
        this.serverless.pluginManager.cliOptions.region = this.options.region ||
            (this.serverless.service.provider && this.serverless.service.provider.region) ||
            'us-east-1';
        }
        this.serverless.variables.populateService(this.serverless.pluginManager.cliOptions);
        return Promise.resolve();
    });
  }
}

module.exports = ServerlessPlugin;
