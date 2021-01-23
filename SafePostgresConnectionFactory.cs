using Microsoft.Extensions.Logging;
using Npgsql;
using RepositoryPattern.Abstractions;
using RepositoryPattern.Abstractions.Wrappers;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace RepositoryPattern.Postgres
{
    public class SafePostgresConnectionFactory : IXPostgresFactory
    {
        private readonly DbConnectionStringBuilder _connectionBuilder;
        private readonly ILogger<PostgresConnectionFactory> _logger;
        private NpgsqlConnection? _connection;
        private bool _committed;

        public SafePostgresConnectionFactory(ILoggerFactory loggerFactory, string connectionString)
        {
            _logger = loggerFactory.CreateLogger<PostgresConnectionFactory>();
            try
            {
                _connectionBuilder = new DbConnectionStringBuilder
                {
                    ConnectionString = connectionString
                };
            }
            catch (Exception ex)
            {
                throw new DataRepositoryException(DataRepositoryException.INVALID_CONNECTION_STRING, ex);
            }
            _logger.LogTrace("Valid connection string retrieved from DB");
        }

        public SafePostgresConnectionFactory(ILoggerFactory loggerFactory, TenantDBProvider provider)
        {
            _logger = loggerFactory.CreateLogger<PostgresConnectionFactory>();
            _logger.LogTrace("Connection string retrieved from provider");

            try
            {
                _connectionBuilder = new DbConnectionStringBuilder
                {
                    ConnectionString = provider.RetrieveConnectionString()
                };
            }
            catch (Exception ex)
            {
                throw new DataRepositoryException(DataRepositoryException.INVALID_CONNECTION_STRING, ex);
            }
        }

        public override bool IsSchemaValid(string schemaName)
        {
            using var _schemaChecker = new NpgsqlConnection(_connectionBuilder.ConnectionString);
            _schemaChecker.Open();
            var command = _schemaChecker.CreateCommand();
            command.CommandText = "SELECT schema_name FROM information_schema.schemata where schemaName = @schemaName";
            command.Parameters.AddWithValue("@schemaName", schemaName);
            using var reader = command.ExecuteReader();
            if (reader is null || !reader.HasRows)
                return false;
            else
                return true;
        }

        public override async Task<XDbConnection> CreateOpenConnectionAsync(string schemaName)
        {
            if (!string.IsNullOrEmpty(schemaName))
                _connectionBuilder.Add("search path", schemaName);

            return await CreateOpenConnectionAsync();
        }


        public override async Task<XDbConnection> CreateOpenConnectionAsync()
        {
            return (XDbConnection)await ProvisionConnection();
        }

        private async Task<IDbConnection> ProvisionConnection()
        {
            if (_connection is null)
            {
                _logger.LogDebug("Initiating connection");
                _connection = new NpgsqlConnection(_connectionBuilder.ConnectionString);
                _connection.Disposed += ConnectionDisposed;
            }

            if (_connection.State != ConnectionState.Open)
            {
                _logger.LogDebug("Connection hasn't opened yet or it was closed");
                await _connection.OpenAsync();
            }
            return _connection;
        }

        private void ConnectionDisposed(object sender, EventArgs e)
        {
            //To avoid opening a connection which has been disposed. This shouldn't happen with new IXDbConnection as it's dispose has been overridden
            _connection = null;
        }

        public bool IsDeadlockException(Exception ex)
        {
            return ex != null
&& (ex is DbException
                && ex.Message.Contains("deadlock")
|| (ex.InnerException != null
&& IsDeadlockException(ex.InnerException)));

        }

        // Flag: Has Dispose already been called?
        bool disposed = false;

        // Public implementation of Dispose pattern callable by consumers.
        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                if (_connection is object)
                {
                    if (CurrentTransaction is object)
                    {
                        if (!_committed)
                            CurrentTransaction.Rollback();
                        CurrentTransaction.Dispose();
                    }
                    _connection.Dispose();
                }
            }
            disposed = true;
        }

        protected override async Task Initiate(IsolationLevel isolationLevel)
        {
            var connection = await ProvisionConnection();
            _currentTransaction = connection.BeginTransaction(isolationLevel);
        }

        protected override Task Commit()
        {
            if (_currentTransaction is object)
            {
                _currentTransaction.Commit();
                _committed = true;
            }
            return Task.CompletedTask;
        }

        ~SafePostgresConnectionFactory()
        {
            Dispose(false);
        }
    }
}
