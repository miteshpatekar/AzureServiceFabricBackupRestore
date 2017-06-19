using BackupActor.Interfaces;
using Microsoft.ServiceFabric.Actors.Runtime;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using StatefulBackupService;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BackupActor
{
    public class BackupService : ActorService, IBackupActorService
    {
        private AzureBlobBackupManager backupManager;
        StatefulServiceContext _context;
        public BackupService(StatefulServiceContext context, ActorTypeInformation actorTypeInfo)
            : base(context, actorTypeInfo, null, null, new KvsActorStateProvider(true)) // Enable incremental backup
        {
            _context = context;
        }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]
            {
                new ServiceReplicaListener(context => this.CreateServiceRemotingListener(context))
            };
        }

        protected override async Task<bool> OnDataLossAsync(RestoreContext restoreCtx, CancellationToken cancellationToken)
        {
            // ServiceEventSource.Current.ServiceMessage(this.Context, "OnDataLoss Invoked!");
            this.SetupBackupManager();

            try
            {
                string backupFolder;


                backupFolder = await this.backupManager.RestoreLatestBackupToTempLocation(cancellationToken);


                //  ServiceEventSource.Current.ServiceMessage(this.Context, "Restoration Folder Path " + backupFolder);

                RestoreDescription restoreRescription = new RestoreDescription(backupFolder, RestorePolicy.Force);

                await restoreCtx.RestoreAsync(restoreRescription, cancellationToken);

                // ServiceEventSource.Current.ServiceMessage(this.Context, "Restore completed");

                DirectoryInfo tempRestoreDirectory = new DirectoryInfo(backupFolder);
                tempRestoreDirectory.Delete(true);

                return true;
            }
            catch (Exception e)
            {
                //ServiceEventSource.Current.ServiceMessage(this.Context, "Restoration failed: " + "{0} {1}" + e.GetType() + e.Message);

                throw;
            }
        }

        public async Task PeriodicTakeBackupAsync()
        {
            long backupsTaken = 0;
            this.SetupBackupManager();

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(this.backupManager.backupFrequencyInSeconds));

                BackupDescription backupDescription = new BackupDescription(BackupOption.Full, this.BackupCallbackAsync);

                await this.BackupAsync(backupDescription);

                backupsTaken++;

                //ServiceEventSource.Current.ServiceMessage(this.Context, "Backup {0} taken", backupsTaken);

            }
        }

        private async Task<bool> BackupCallbackAsync(BackupInfo backupInfo, CancellationToken cancellationToken)
        {
            //ServiceEventSource.Current.ServiceMessage(this.Context, "Inside backup callback for replica {0}|{1}", this.Context.PartitionId, this.Context.ReplicaId);
            long totalBackupCount;

            // IReliableDictionary<string, long> backupCountDictionary =
            //    await this.StateProvider.GetOrAddAsync<IReliableDictionary<string, long>>(BackupCountDictionaryName);
            //IReliableDictionary<string, long> backupCountDictionary =
            //    await _context
            //using (ITransaction tx = this.StateManager.CreateTransaction())
            //{
            //    ConditionalValue<long> value = await backupCountDictionary.TryGetValueAsync(tx, "backupCount");

            //    if (!value.HasValue)
            //    {
            //        totalBackupCount = 0;
            //    }
            //    else
            //    {
            //        totalBackupCount = value.Value;
            //    }

            //    await backupCountDictionary.SetAsync(tx, "backupCount", ++totalBackupCount);

            //    await tx.CommitAsync();
            //}

            // ServiceEventSource.Current.Message("Backup count dictionary updated, total backup count is {0}", totalBackupCount);

            try
            {
                // ServiceEventSource.Current.ServiceMessage(this.Context, "Archiving backup");
                await this.backupManager.ArchiveBackupAsync(backupInfo, cancellationToken);
                // ServiceEventSource.Current.ServiceMessage(this.Context, "Backup archived");
            }
            catch (Exception e)
            {
                // ServiceEventSource.Current.ServiceMessage(this.Context, "Archive of backup failed: Source: {0} Exception: {1}", backupInfo.Directory, e.Message);
            }

            await this.backupManager.DeleteBackupsAsync(cancellationToken);

            //  ServiceEventSource.Current.Message("Backups deleted");

            return true;
        }


        private void SetupBackupManager()
        {
            ICodePackageActivationContext codePackageContext = this.Context.CodePackageActivationContext;
            string partitionId = this.Context.PartitionId.ToString("N");
            long minKey = ((Int64RangePartitionInformation)this.Partition.PartitionInfo).LowKey;
            long maxKey = ((Int64RangePartitionInformation)this.Partition.PartitionInfo).HighKey;

            if (this.Context.CodePackageActivationContext != null)
            {
                this.backupManager = new AzureBlobBackupManager(partitionId, minKey, maxKey, codePackageContext.TempDirectory);
            }
        }
    }
}
