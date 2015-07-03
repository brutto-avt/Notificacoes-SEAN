(function () {
    'use strict';
    var express = require('express'),
        app = express(),
        mssql = require('mssql'),
        server = require('http').Server(app),
        io = require('socket.io')(server),
        config = {
            user: 'sa',
            password: '1',
            server: '127.0.0.1',
            database: 'agenda'
        };

    server.listen(80);

    app.locals.compromissos = [];
    var atualizaCompromissos = function () {
        var connection = new mssql.Connection(config, function (err) {
            var request = new mssql.Request(connection);
            request.execute('dbo.spCompromissos', function (err, recordset, returnValue) {
                app.locals.compromissos = recordset[0];
            });
        });
    };
    atualizaCompromissos();

    app.use(express.static('public'));

    app.get('/', function (req, res) {
        res.sendFile(__dirname + '/public/templates/index.html');
    });

    app.get('/atualizar', function (req, res) {
        atualizaCompromissos();
        console.log('Atualizando compromissos...');
        res.sendStatus(200);
    });

    io.on('connection', function (socket) {
        socket.emit('notificacoes', app.locals.compromissos);
        socket.on('atualizaNotificacoes', function (params) {
            atualizaCompromissos();
            socket.emit('notificacoes', app.locals.compromissos);
        });
    });
})();