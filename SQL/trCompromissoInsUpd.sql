create trigger dbo.trCompromissoInsUpd
on dbo.compromisso
after insert, update, delete
as
begin
	declare @objHTTP int;
	declare @url varchar(2048) = 'http://localhost/atualizar';

	exec sp_OACreate 'Microsoft.XMLHTTP', @objHTTP out;
	exec sp_OAMethod @objHTTP, 'Open', NULL, 'GET', @url, 0;
	exec sp_OAMethod @objHTTP, 'Send';
end