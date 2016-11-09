"use strict";

import express from 'express';
import mqtt from 'mqtt';
import uuid from 'node-uuid';
import rp from 'request-promise';

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
        client.subscribe('crimethory');
        socket.emit('connected');
    });

    client.on('message', function (topic, message) {
        console.log(topic, message.toString());
        if (topic === 'crimethory') {
            let result;
            try {
                result = JSON.parse(message.toString());
                if (result && result['request'] && runningJobs[result['request']['externalId']] === socket) {
                    if (result['success']) {
                        socket.emit("got stat", result["payload"]);
                    }
                    delete runningJobs[result['request']['externalId']];
                }
                if (result && result['text'] && result['id']) {
                    console.log("new tweet: ", result);
                    socket.emit("new tweet", result);
                }
            } catch (exc) {
                console.error("error while parsing json from mqtt: ", exc);
            }
        }
    });

    socket.on('get stat', ({ lat, lng, month }) => {
        const options = {
            uri: 'http://mist:2003/api/train',
            json: true,
            body: {
                lat: lat.toFixed(4),
                lng: lng.toFixed(4),
                month
            }
        };
        rp(options).then(() => {
            const uid = uuid.v1();
            const request = {
                route: "stats",
                parameters: {
                    lat: lat.toFixed(4),
                    lng: lng.toFixed(4),
                    month
                },
                externalId: uid
            };
            runningJobs[uid] = socket;
            client.publish("crimethory", JSON.stringify(request));
        }).catch((e) => console.error(e));
    });
    
    socket.on('diconnect', () => {
        client.end();
    });
});
