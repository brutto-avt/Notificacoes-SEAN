create procedure spCompromissos
as
begin
	select id, descricao, data_hora
	from dbo.compromisso
	where datediff(second, data_hora, current_timestamp) <= 5;
end
