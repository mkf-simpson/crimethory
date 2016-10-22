"use strict";

import express from 'express';
import mqtt from 'mqtt';

const app = express();
app.use(express.static('public'));

var http = require('http').Server(app);

var io = require('socket.io')(http);

http.listen(3000, () => {
    console.log("Ready!");
});

io.on('connection', (socket) => {
    var client  = mqtt.connect('mqtt://mosquitto:1888');

    console.log(`New client: ${socket.id}`);
    client.on('connect', function () {
        console.log("connected to mqtt");
        client.subscribe('crimethory');
    });

    client.on('message', function (topic, message) {
        console.log(topic, message.toString());
        if (topic === 'crimethory') {
            socket.emit('mqtt message', message.toString());
        }
    });
});
