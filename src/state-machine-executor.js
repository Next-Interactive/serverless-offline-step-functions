const Promise = require('bluebird');
const child_process = require('child_process');
const _ = require('lodash');
const fs = require('fs');
const jsonPath = require('JSONPath');
const choiceProcessor = require('./choice-processor');
const stateTypes = require('./state-types');
const StateRunTimeError = require('./state-machine-error');
const createLambdaContext = require('../node_modules/serverless-offline/src/createLambdaContext');
const objectPath = require("object-path");

const logPrefix = '[Serverless Offline Step Functions]:';

class StateMachineExecutor {
    constructor(stateMachineName, stateName, stateMachineJSONInput) {
        this.currentStateName = stateName;
        this.stateMachineName = stateMachineName;
        this.stateMachineJSON = {};
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
    spawnProcess(stateInfo, input, context, callback = null) {
        console.log(`* * * * * ${this.currentStateName} * * * * *`);
        let globalInput = input

        // This will be used as the parent node key for when the process
        // finishes and its output needs to be processed.
        const outputKey = `sf-${Date.now()}`;

        input = this.processTaskInputPath(input, stateInfo);
        input = this.processTaskParameters(input, stateInfo);
        console.log('input: ', input);

        const child = child_process.spawn('node',
        [
            '-e',
            this.whatToRun(stateInfo, input, outputKey, callback)],
            { stdio: 'pipe',
            env: Object.assign({}, process.env, {
                input: JSON.stringify(input),
            })});

            let outputData = null;
            child.stdout.on('data', (data) => {
                if (Buffer.isBuffer(data) &&
                stateInfo.Type !== stateTypes.FAIL) {
                    data = data.toString().trim();

                    try {
                        // check that output is a JSON string
                        const parsed = JSON.parse(data);
                        // only process data if sent as step functions output so data from
                        // other console.logs don't get processed
                        if (typeof parsed[outputKey] !== 'undefined') {
                            outputData = parsed[outputKey];
                        }
                    } catch(error) {
                        console.log(`${logPrefix} error processing data: ${data}`);
                    }
                }
            });


            child.stderr.on('data', (data) => {
                console.error(`${logPrefix} stderr:`, data.toString())
            });

            child.on('exit', () => {
                let output = null;
                // any state except the fail state may have OutputPath
                if(stateInfo.Type !== 'Fail') {
                    // state types Parallel, Pass, and Task can generate a result and can include ResultPath
                    if([stateTypes.PARALLEL, stateTypes.PASS, stateTypes.TASK].indexOf(stateInfo.Type) > -1) {
                        globalInput = this.processTaskResultPath(globalInput, stateInfo, (outputData || {}));
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

                // const newEvent = event ? Object.assign({}, event) : {};
                // newEvent.input = event.output;
                // newEvent.stateName = stateInfo.Next;
                this.currentStateName = stateInfo.Next;
                stateInfo = this.stateMachineJSON.stateMachines[this.stateMachineName].definition.States[stateInfo.Next];
                console.log('output: ', output);
                this.spawnProcess(stateInfo, output, context);
            });
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
    whatToRun(stateInfo, input, outputKey, callback) {
        switch(stateInfo.Type) {
            case 'Task':
                // TODO: catch, retry
                // This will spin up a node child process to fire off the handler function of the given lambda
                // the output of the lambda is placed into a JSON object with the outputKey generated above as
                // the parent node and piped to stdout for processing. This is done so other console.logs are not
                // processed by this plugin.
                // process.exit(0) must be called in .then because a child process will not exit if it has connected
                // to another resource, such as a database or redis, which may be a source of future events.
                const handlerSplit = stateInfo.handler.split('.');
                // const cb = callback(null, { statusCode: 200, body: JSON.stringify({ startDate: sme.startDate, executionArn: sme.executionArn }) });
                // const context = ;

                let runner = `const context = require('./node_modules/serverless-offline/src/createLambdaContext')(require('./.build/${handlerSplit[0]}').${handlerSplit[1]}, ${callback}); `;
                runner += `require("./.build/${handlerSplit[0]}").${handlerSplit[1]}(JSON.parse(process.env.input), context, ${callback})`;
                runner += `.then((data) => { console.log(JSON.stringify({ "${outputKey}": data || {} })); process.exit(0); })`;
                runner += `.catch((e) => { console.error("${logPrefix} handler error:",e); })`;
                return runner;

            // should pass input directly to output without doing work
            case 'Pass':
                if (stateInfo.Result !== undefined) {
                    return `console.log(JSON.stringify({ "${outputKey}": ${JSON.stringify(stateInfo.Result)} || {} })); process.exit(0);`;
                }

                return '';
            // Waits before moving on:
            // - Seconds, SecondsPath: wait the given number of seconds
            // - Timestamp, TimestampPath: wait until the given timestamp
            case 'Wait':
                return this.buildWaitState(stateInfo, input);
            // ends the state machine execution with 'success' status
            case 'Succeed':
            // ends the state machine execution with 'fail' status
            case 'Fail':
                return this.endStateMachine(null, stateInfo);
            // adds branching logic to the state machine
            case 'Choice':
                stateInfo.Next = choiceProcessor.processChoice(stateInfo, input);
                return '';
            case 'Parallel':
                return `console.error('${logPrefix} 'Parallel' state type is not yet supported by serverless offline step functions')`;
            default:
                return `console.error('${logPrefix} Invalid state type: ${stateInfo.Type}')`
        }
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

        return `setTimeout(() => {}, ${+milliseconds*1000});`;
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
                if (Array.isArray(data)) {
                    input = [...data]
                } else if (typeof data === 'object') {
                    input = Object.assign({}, data)
                } else {
                    input = data
                }
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
    processTaskResultPath(input, stateInfo, resultData) {
        // according to AWS docs:
        // ResultPath (Optional)
        // A path that selects a portion of the state's input to be passed to the state's output.
        // If omitted, it has the value $ which designates the entire input.
        // For more information, see Input and Output Processing.
        // https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-common-fields.html
        if (typeof stateInfo.ResultPath === 'undefined' || stateInfo.ResultPath === '$') {            
            return resultData
        }

        if (stateInfo.ResultPath === null) {
            return input
        }

        const resultPathArray = jsonPath.toPathArray(stateInfo.ResultPath)
        if (resultPathArray[0] === '$' && resultPathArray.length > 1) {resultPathArray.shift();}

        objectPath.set(input, resultPathArray, resultData)

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
