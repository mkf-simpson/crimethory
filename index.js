"use strict";

import express from 'express';
import mqtt from 'mqtt';
import uuid from 'node-uuid';

const app = express();
app.use(express.static('public'));

var http = require('http').Server(app);

var io = require('socket.io')(http);

http.listen(3000, () => {
    console.log("Ready!");
});

const runningJobs = {};

io.on('connection', (socket) => {
    var client  = mqtt.connect('mqtt://mosquitto:1883');

    console.log(`New client: ${socket.id}`);
    client.on('connect', function () {
        console.log("connected to mqtt");
        client.subscribe('foo');
        socket.emit('connected');
    });

    client.on('message', function (topic, message) {
        console.log(topic, message.toString());
        if (topic === 'foo') {
            let result;
            try {
                result = JSON.parse(message.toString());
            } catch (exc) {
                console.error(exc);
            }
            if (result && result['request'] && runningJobs[result['request']['external_id']] === socket) {
                if (result['success']) {
                    socket.emit("got stat", result["payload"]);
                }
                delete runningJobs[result['request']['external_id']];
            }
        }
    });

    socket.on('get stat', ({ lat, lng, month }) => {
        const uid = uuid.v1();
        const request = {
            path: "/jobs/crimethory_2.10-0.0.10.jar",
            className: "CrimeByPoint$",
            namespace: "crime-requested-jobs",
            parameters: {
                lat: lat.toFixed(4),
                lng: lng.toFixed(4),
                month
            },
            external_id: uid
        };
        runningJobs[uid] = socket;
        client.publish("foo", JSON.stringify(request));
    });
});
