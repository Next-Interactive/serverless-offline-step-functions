const _ = require('lodash');
const fs = require('fs');
const jsonPath = require('JSONPath');
const choiceProcessor = require('./choice-processor');
const AWSServicesProcessor = require('./aws-services-processor');
const stateTypes = require('./state-types');
const StateRunTimeError = require('./state-machine-error');
const objectPath = require("object-path");
const lambdaContext = require(process.cwd() + '/node_modules/serverless-offline/src/createLambdaContext')
const clonedeep = require('lodash.clonedeep')

const logPrefix = '[Serverless Offline Step Functions]:';

class StateMachineExecutor {
    constructor(config, awsServices, stateMachineName, stateName, stateMachineJSONInput, provider) {
        this.awsServicesProcessor = new AWSServicesProcessor(config)
        this.awsServices = awsServices;
        this.currentStateName = stateName;
        this.stateMachineName = stateMachineName;
        this.stateMachineJSON = {};
        this.provider = provider;

        if (stateMachineJSONInput) {
            this.stateMachineJSON.stateMachines = _.assign({}, this.stateMachineJSON.stateMachines, stateMachineJSONInput);
        } else if (fs.existsSync('./state-machine.json')) {
            this.stateMachineJSON = require('./step-functions.json');
        }
        // step execution response includes the start date
        this.startDate = Date.now();
        // step execution response includes the execution ARN
        // use this for now to give a unique id locally
        this.executionArn = `${stateMachineName}-${stateName}-${this.startDate}`;
    }

    /**
     * Spawns a new process to run a given State from a state machine
     * @param {*} stateInfo
     * @param {*} input
     * @param {*} context
     */
    spawnProcess(stateInfo, input, context, callback = null, retryNumber = 0) {
        console.log(`* * * * * ${this.currentStateName} * * * * *`);
        let globalInput = input

        input = this.processTaskInputPath(input, stateInfo);
        input = this.processTaskParameters(input, stateInfo);
        console.log('input: ', input);

        this.whatToRun(stateInfo, input, callback)
            .then((data) => {
                let output = null;
                // any state except the fail state may have OutputPath
                if(stateInfo.Type !== 'Fail') {
                    // state types Parallel, Pass, and Task can generate a result and can include ResultPath
                    if([stateTypes.PARALLEL, stateTypes.PASS, stateTypes.TASK].indexOf(stateInfo.Type) > -1) {
                        globalInput = this.processTaskResultPath(globalInput, stateInfo.ResultPath, (data || {}));
                    }

                    // NOTE:
                    // State machine data is represented by JSON text, so you can provide values using any data type supported by JSON
                    // https://docs.aws.amazon.com/step-functions/latest/dg/concepts-state-machine-data.html
                    output = this.processTaskOutputPath(globalInput, stateInfo.OutputPath);
                }
                // kick out if it is the last one (end => true) or state is 'Success' or 'Fail
                if (stateInfo.Type === 'Succeed' || stateInfo.Type === 'Fail' || stateInfo.End === true) {
                    return this.endStateMachine(null, null, output);
                }

                this.goToNextStep(stateInfo.Next, output, context, callback)
            })
            .catch((error) => {
                if (stateInfo.Retry !== undefined) {
                    retryNumber++
                    const maxAttempts = (stateInfo.Retry[0].MaxAttempts !== undefined ? stateInfo.Retry[0].MaxAttempts : 3)
                    
                    if (retryNumber <= maxAttempts) {
                        console.log('Retry', retryNumber)
                        this.spawnProcess(stateInfo, globalInput, context, callback, retryNumber);

                        return
                    }
                }
                
                const errorJSON = {
                    'Error': error.name,
                    'Cause': {
                        "errorType": error.name,
                        "errorMessage": error.message,
                        "trace": error.stack
                    }
                }

                if (stateInfo.Catch !== undefined) {
                    console.log('Catch')
                    globalInput = this.processTaskResultPath(globalInput, stateInfo.Catch[0].ResultPath, (errorJSON || {}));
                    this.goToNextStep(stateInfo.Catch[0].Next, globalInput, context, callback)

                    return
                }

                this.endStateMachine(errorJSON, null, globalInput) 
            })
    }

    goToNextStep(next, output, context, callback) {
        this.currentStateName = next;
        const nextStateInfo = this.stateMachineJSON.stateMachines[this.stateMachineName].definition.States[next];
        console.log('output: ', output);
        this.spawnProcess(nextStateInfo, output, context, callback);
    }

    endStateMachine(error, input, output, message) {
        if(error) {
            console.error(`${logPrefix} Error:`, error);
        } else {
            console.log(`${logPrefix} State Machine Completed`);
        }

        if (message) {
            console.log(`${logPrefix}`, message);
        }

        if(input) {
            console.log(`${logPrefix} input:`, input);
        }

        console.log(`${logPrefix} output:`, output);
        return true;
    }

    /**
     * decides what to run based on state type
     * @param {object} stateInfo
     */
    whatToRun(stateInfo, input, callback) {
        return new Promise((resolve, reject) => {
            switch(stateInfo.Type) {
                case 'Task':
                    if (this.awsServices.includes(stateInfo.Resource)) {
                        return resolve(this.awsServicesProcessor.processAWSServices(stateInfo, input))
                    }
                    
                    if (stateInfo.environment !== undefined) {
                        Object.keys(stateInfo.environment).forEach(function(key){
                            process.env[key] = stateInfo.environment[key]
                        });
                    }

                    const handlerSplit = stateInfo.handler.split('.');
                    const context = lambdaContext(require(process.cwd() + '/.build/' + handlerSplit[0])[handlerSplit[1]], JSON.stringify(this.provider), callback)
                    const currentLambda = require(process.cwd() + '/.build/' + handlerSplit[0])[handlerSplit[1]](input, context, callback)
                    
                    return currentLambda
                        .then((data) => { return resolve(data)})
                        .catch((error) => {
                            return reject(error)
                        })
                // should pass input directly to output without doing work
                case 'Pass':
                    if (stateInfo.Result !== undefined) {
                        return resolve(clonedeep(stateInfo.Result))
                    }
    
                    return resolve()
                // Waits before moving on:
                // - Seconds, SecondsPath: wait the given number of seconds
                // - Timestamp, TimestampPath: wait until the given timestamp
                case 'Wait':
                    setTimeout(() => {
                        return resolve()
                    }, this.buildWaitStateTmp(stateInfo, input))
                // ends the state machine execution with 'success' status
                case 'Succeed':
                // ends the state machine execution with 'fail' status
                case 'Fail':
                    return resolve(this.endStateMachine(null, stateInfo));
                // adds branching logic to the state machine
                case 'Choice':
                    stateInfo.Next = choiceProcessor.processChoice(stateInfo, input);
                    return resolve();
                case 'Parallel':
                    return `console.error('${logPrefix} 'Parallel' state type is not yet supported by serverless offline step functions')`;
                default:
                    return `console.error('${logPrefix} Invalid state type: ${stateInfo.Type}')`
            }
        })
    }
    
    buildWaitState(stateInfo, event) {
        let milliseconds = 0;
        // SecondsPath: specified using a path from the state's input data.
        if ((stateInfo.Seconds && !_.isNaN(+stateInfo.Seconds))) {
            milliseconds = +stateInfo.Seconds
        } else if (stateInfo.SecondsPath && event.input) {
            milliseconds = +jsonPath({ json: event.input, path: stateInfo.SecondsPath })[0];
        }

        if (_.isNaN(milliseconds)) {
            return ''+ this.endStateMachine(
                new StateRunTimeError('Specified wait time is not a number'), stateInfo);
        }

        return milliseconds*1000
    }

    /**
     * Process the state's InputPath - per AWS docs:
     * The InputPath field selects a portion of the state's input to pass to the state's
     * task for processing. If you omit the field, it gets the $ value, representing the
     * entire input. If you use null, the input is discarded (not sent to the state's
     * task) and the task receives JSON text representing an empty object {}.
     * https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-input-output-processing.html
     * @param {*} input
     * @param {*} stateInfo
     */
    processTaskInputPath(input, stateInfo) {
        stateInfo.InputPath = typeof stateInfo.InputPath === 'undefined' ? '$' : stateInfo.InputPath;
        if (stateInfo.InputPath === null) {
            input = {};
        } else {
            input = input ? input : {};
            jsonPath({ json: input, path: stateInfo.InputPath, callback: (data) => {
                input = clonedeep(data)
            }});
        }

        return input
    }

    /**
     * Process the state's Parameters - per AWS docs:
     * https://docs.aws.amazon.com/step-functions/latest/dg/input-output-inputpath-params.html
     * @param {*} input
     * @param {*} stateInfo
     */
    processTaskParameters(input, stateInfo) {
        if (typeof stateInfo.Parameters === 'undefined') {
            return input
        }

        return this.handleParameters(input, stateInfo.Parameters)
    }

    handleParameters(input, params) {
        let returnParam = {}
        Object.keys(params).forEach((key) => {
            if (key.endsWith('.$')) {
                returnParam[key.substr(0, key.length - 2)] = jsonPath({ json: input, path: params[key]})[0]
            } else if (typeof params[key] === 'object') {
                returnParam[key] = this.handleParameters(input, params[key])
            } else {
                returnParam[key] = params[key]
            }
        })

        return returnParam
    }

    /**
     * Moves the result of the task to the path specified by ResultPath in
     * the task's input according to the state's config.
     * AWS docs on processing of input/output:
     * https://docs.aws.amazon.com/step-functions/latest/dg/input-output-paths.html
     * @param {*} event
     * @param {*} stateInfo
     * @param {string} resultData
     */
    processTaskResultPath(input, resultPath, resultData) {
        // according to AWS docs:
        // ResultPath (Optional)
        // A path that selects a portion of the state's input to be passed to the state's output.
        // If omitted, it has the value $ which designates the entire input.
        // For more information, see Input and Output Processing.
        // https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-common-fields.html
        if (typeof resultPath === 'undefined' || resultPath === '$') {
            return resultData
        }

        if (resultPath !== null) {
            const resultPathArray = jsonPath.toPathArray(resultPath)
            if (resultPathArray[0] === '$' && resultPathArray.length > 1) {resultPathArray.shift();}

            objectPath.set(input, resultPathArray, resultData)
        }

        return input
    }

    /**
     * OutputPath:
     * If OutputPath is **NOT** specified, the entire (original) input is set to output
     * If OutputPath is specified, only the specified node (from the input) is returned
     * If the OutputPath is null, JSON text representing an empty object {} is sent to the next state.
     * If the OutputPath doesn't match an item in the state's input, an exception specifies an invalid path
     * For more information, see Input and Output Processing.
     * https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-common-fields.html
     * @param {*} event
     * @param {*} stateInfo
     */
    processTaskOutputPath(data, path) {
        let output = null;
        if (path !== null) {
            output = jsonPath({ json: data, path: path || '$', })[0];

            if (!output) {
                return this.endStateMachine(
                    new StateRunTimeError(`An error occurred while executing the state '${this.currentStateName}'. Invalid OutputPath '${path}': The Output path references an invalid value.`),
                    data);
            }
        }

        return output;
    }
}

module.exports = StateMachineExecutor;
