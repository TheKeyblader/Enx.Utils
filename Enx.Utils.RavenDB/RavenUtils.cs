using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Enx
{
    public static class RavenUtils
    {
        public static void EnsureDatabaseExists(this IDocumentStore store, string database, bool createDatabaseIfNotExists = true)
        {
            database ??= store.Database;

            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(database));

            try
            {
                store.Maintenance.ForDatabase(database).Send(new GetStatisticsOperation());
            }
            catch (DatabaseDoesNotExistException)
            {
                if (createDatabaseIfNotExists == false)
                    throw;

                try
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                }
                catch (ConcurrencyException)
                {
                    // The database was already created before calling CreateDatabaseOperation
                }

            }
        }

        public static async Task<bool> EnsureDatabaseExistsAsync(this IDocumentStore store, string database, bool createDatabaseIfNotExists = true, CancellationToken token = default)
        {
            database ??= store.Database;

            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(database));

            try
            {
                await store.Maintenance.ForDatabase(database).SendAsync(new GetStatisticsOperation(), token);
                return false;
            }
            catch (DatabaseDoesNotExistException)
            {
                if (createDatabaseIfNotExists == false)
                    throw;

                try
                {
                    store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(database)));
                    return true;
                }
                catch (ConcurrencyException)
                {
                    // The database was already created before calling CreateDatabaseOperation
                    return true;
                }

            }
        }

        public static string CompleteId<TType>(this IAsyncDocumentSession session, string id)
        {
            var conventions = session.Advanced.DocumentStore.Conventions;
            var prefix = conventions.TransformTypeCollectionNameToDocumentIdPrefix(conventions.GetCollectionName(typeof(TType)));
            prefix += conventions.IdentityPartsSeparator;
            if (id.Contains(prefix)) return id;
            else return prefix + id;
        }

        public static string PartialId(this string id)
        {
            var splits = id.Split('/');
            if (splits.Length == 2) return splits[1];
            else return id;
        }
    }
}
