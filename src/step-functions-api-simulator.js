/* eslint-disable import/no-extraneous-dependencies */
const http = require('http');
const chalk = require('chalk');
const _ = require('lodash');
const StateMachineExecutor = require('./state-machine-executor');
const callback = require('./callback')

module.exports = (serverless, awsServices) => {
    const config = serverless.service.custom['serverless-offline-step-functions'];
    const port = config.port || 8014;

    const logPrefix = chalk.magenta('[Step Functions API Simulator]');

    function log(msg) {
        serverless.cli.log(`${logPrefix} ${msg}`);
    }

    http.createServer((req, res) => {
        log(`Got request for ${req.method} ${req.url}`);
        let dataJSON = '';
        req.on('data', (chunk) => {
            dataJSON += chunk.toString();
            log(chunk.toString());
        });
        req.on('end', () => {
            const data = JSON.parse(dataJSON);
            const machineName = _.last(data.stateMachineArn.split(':'));
            let startDate = null;
            let exeArn = '';
            _.forEach(serverless.service.stepFunctions.stateMachines, (machine, machineKey) => {

                if (machine.name === machineName) {
                    const currentState = machine.definition.States[machine.definition.StartAt];
                    const sme = new StateMachineExecutor(config, awsServices, machineKey, machine.definition.StartAt, { [machineKey]: machine }, serverless.service.provider);

                    // TODO: check integration type to set input properly (i.e. lambda vs. sns)
                    sme.spawnProcess(currentState, JSON.parse(data.input), {}, callback);
                    startDate = sme.startDate;
                    exeArn = sme.executionArn;
                }
            });
            // per docs, step execution response includes the start date and execution arn
            res.writeHead(200, {'Content-Type': 'application/json'});

            res.end(JSON.stringify({
                startDate: startDate,
                executionArn: exeArn,
            }));
        });
    }).listen(port, () => {
        log(`Running at http://127.0.0.1:${port}`);
    });
}