var app = angular.module('NotificaApp', ['io.service']);

app.controller('MainCtr', function ($scope, $filter, $interval, socket) {
    'use strict';
	toastr.options.closeButton = true;
	toastr.options.showEasing = 'swing';
	toastr.options.showMethod = 'slideDown';
	toastr.options.timeOut = 0;
	toastr.options.extendedTimeOut = 0;
	toastr.options.progressBar = true;
	$scope.notificacoes = [];
    
	$interval(function () {
		socket.emit('atualizaNotificacoes', {});
	}, 5000);

	socket.on('notificacoes', function (resposta) {
		angular.forEach(resposta, function (item) {
			if (!$filter('filter')($scope.notificacoes, {id: item.id}).length) {
				$scope.notificacoes.push(item);
				$scope.$apply();
			}
		});
	});

	$scope.$watchCollection("notificacoes", function () {
		angular.forEach($scope.notificacoes, function (notif) {
			toastr.info('<a href="#">' + notif.descricao + '</a>');
		});
	});
});