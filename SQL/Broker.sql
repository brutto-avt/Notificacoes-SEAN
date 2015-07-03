ALTER DATABASE agenda SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
sp_configure 'show advanced options', 1
GO 
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1
GO 
RECONFIGURE;
GO 
sp_configure 'show advanced options', 1
GO 
RECONFIGURE;
GO

CREATE TABLE dbo.tarefa (
	id INT IDENTITY(1,1),
	sql_agendada nvarchar(max) NOT NULL,
	primeira_em datetime NOT NULL,
	ultima_em datetime,
	ultima_ok BIT NOT NULL DEFAULT (0),
	repetivel BIT NOT NULL DEFAULT (0),
	habilitada BIT NOT NULL DEFAULT (0),
	handle uniqueidentifier NULL
)
GO

CREATE TABLE dbo.tarefa_erro (
	id BIGINT IDENTITY(1, 1) PRIMARY KEY,
	linha INT,
	numero INT,
	mensagem NVARCHAR(MAX),
	gravidade INT,
	estado INT,
	tarefa_id INT,
	data DATETIME NOT NULL DEFAULT GETUTCDATE()
)
GO

CREATE PROCEDURE dbo.spcTarefaDel @pTarefa_id INT
AS	
	BEGIN TRANSACTION
	BEGIN TRY
		DECLARE @vHandle UNIQUEIDENTIFIER

		-- encontra o handle da tarefa
		SELECT	@vHandle = handle
		FROM dbo.tarefa
		WHERE id = @pTarefa_id
		
		IF @@ROWCOUNT = 0
			RETURN;
		
		-- finaliza a conversa caso esteja ativa
		IF EXISTS (SELECT * FROM sys.conversation_endpoints WHERE conversation_handle = @vHandle)
			END CONVERSATION @vHandle
		
		-- deleta a tarefa
		DELETE dbo.tarefa
		WHERE id = @pTarefa_id
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN 
			ROLLBACK;
		END

		INSERT INTO dbo.tarefa_erro(
			linha,
			numero,
			mensagem,
			gravidade,
			estado,
			tarefa_id
		)
		SELECT	ERROR_LINE(),
				ERROR_NUMBER(),
				'dbo.spcTarefaDel: ' + ERROR_MESSAGE(),
				ERROR_SEVERITY(),
				ERROR_STATE(),
				@pTarefa_id
	END CATCH
GO

CREATE PROCEDURE dbo.spcTarefaIns @pSql_agendada NVARCHAR(MAX), 
								  @pPrimeira_em DATETIME, 
							      @pRepetivel BIT
AS
	DECLARE @vTarefaId INT, @vTimeout INT, @vHandle UNIQUEIDENTIFIER;
	BEGIN TRANSACTION
	BEGIN TRY
		-- adiciona nova tarefa
		INSERT INTO dbo.tarefa(
			sql_agendada,
			primeira_em,
			repetivel,
			handle
		)
		VALUES (
			@pSql_agendada,
			@pPrimeira_em,
			@pRepetivel,
			NULL
		)
		SELECT @vTarefaId = SCOPE_IDENTITY()
		
		-- define o timeout em segundos
		SELECT @vTimeout = DATEDIFF(SECOND, GETDATE(), @pPrimeira_em);

		-- begin a conversation for our scheduled job
		BEGIN DIALOG CONVERSATION @vHandle
			FROM SERVICE [//ServicoTarefas]
			TO SERVICE '//ServicoTarefas', 'CURRENT DATABASE'
			ON CONTRACT [//ContratoTarefas]
			WITH ENCRYPTION = OFF;

		-- inicia o timer
		BEGIN CONVERSATION TIMER (@vHandle)
			TIMEOUT = @vTimeout;

		-- atualiza tarefa com o handler
		UPDATE dbo.tarefa
		SET	handle = @vHandle,
			habilitada = 1
		WHERE ID = @vTarefaId

		IF @@TRANCOUNT > 0
		BEGIN 
			COMMIT;
		END
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
			BEGIN 
				ROLLBACK;
			END

			INSERT INTO dbo.tarefa_erro (
					linha,
					numero,
					mensagem,
					gravidade,
					estado,
					tarefa_id
			)
			SELECT	ERROR_LINE(),
					ERROR_NUMBER(),
					'dbo.spcTarefaIns: ' + ERROR_MESSAGE(),
					ERROR_SEVERITY(),
					ERROR_STATE(),
					@vTarefaId
		END CATCH
GO

CREATE PROCEDURE dbo.spcExecutaTarefa
AS
	DECLARE @vHandle UNIQUEIDENTIFIER, @vTarefaId INT, @vUltimaEm DATETIME, @vHabilitada BIT, @vUltimaOk BIT
	
	SELECT	@vUltimaEm = GETDATE(),
			@vHabilitada = 0,
			@vUltimaOk = 0
	
	BEGIN TRY
		DECLARE @vTipoMensagem sysname;

		-- recebe uma mensagem da queue
		RECEIVE TOP(1) @vHandle = conversation_handle,
				@vTipoMensagem = message_type_name
		FROM QueueTarefas
	
		IF @@ROWCOUNT = 0 OR ISNULL(@vTipoMensagem, '') != 'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer'
			RETURN;
		
		DECLARE @vSqlAgendada NVARCHAR(MAX), @vRepetivel BIT				
		-- pega a tarefa associada ao handler
		SELECT	@vTarefaId = id,
				@vSqlAgendada = sql_agendada,
				@vRepetivel = repetivel
		FROM dbo.tarefa
		WHERE handle = @vHandle
		AND habilitada = 1
		
		-- end the conversation if it's non repeatable
		IF @vRepetivel = 0
		BEGIN			
			END CONVERSATION @vHandle
			SELECT @vHabilitada = 0
		END
		ELSE
		BEGIN 
			-- reinicia o timer para o proximo minuto
			BEGIN CONVERSATION TIMER (@vHandle)
				TIMEOUT = 60;
			SELECT @vHabilitada = 1
		END

		-- roda a SQL
		EXEC (@vSqlAgendada)
		
		SELECT @vUltimaOk = 1
	END TRY
	BEGIN CATCH
		SELECT @vHabilitada = 0
		
		INSERT INTO dbo.tarefa_erro (
			linha,
			numero,
			mensagem,
			gravidade,
			estado,
			tarefa_id
		)
		SELECT ERROR_LINE(),
			   ERROR_NUMBER(),
			   'dbo.spcExecutaTarefa: ' + ERROR_MESSAGE(), 
			   ERROR_SEVERITY(),
			   ERROR_STATE(),
			   @vTarefaId
		
		-- finaliza conversa em caso de erro
		IF @vHandle != NULL		
		BEGIN
			IF EXISTS (SELECT * FROM sys.conversation_endpoints WHERE conversation_handle = @vHandle)
				END CONVERSATION @vHandle
		END
	END CATCH;

	-- atualiza tarefa
	UPDATE dbo.tarefa
	SET ultima_em = @vUltimaEm,
		habilitada = @vHabilitada,
		ultima_ok = @vUltimaOk
	WHERE id = @vTarefaId
GO

CREATE CONTRACT [//ContratoTarefas] ([http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer] SENT BY INITIATOR)
GO

CREATE QUEUE QueueTarefas
	WITH STATUS = ON,
	ACTIVATION (	
		PROCEDURE_NAME = dbo.spcExecutaTarefa,
		MAX_QUEUE_READERS = 20, -- limite de tarefas simultaneas
		EXECUTE AS 'dbo');
GO

CREATE SERVICE [//ServicoTarefas] 
	AUTHORIZATION dbo
	ON QUEUE QueueTarefas ([//ContratoTarefas])
GO