using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using BackupActor.Interfaces;
using Microsoft.ServiceFabric.Data;
using System.IO;
using Microsoft.ServiceFabric.Actors.Client;
using Microsoft.ServiceFabric.Actors;

namespace BackupRestoreStatefulService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class BackupRestoreStatefulService : StatefulService
    {
        private AzureBlobBackupManager backupManager;
        private const string BackupCountDictionaryName = "BackupCountingDictionary";

        public BackupRestoreStatefulService(StatefulServiceContext context)
            : base(context)
        { }

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        protected override async Task<bool> OnDataLossAsync(RestoreContext restoreCtx, CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(this.Context, "OnDataLoss Invoked!");
            this.SetupBackupManager();

            try
            {
                string backupFolder;

                backupFolder = await this.backupManager.RestoreLatestBackupToTempLocation(cancellationToken);

                ServiceEventSource.Current.ServiceMessage(this.Context, "Restoration Folder Path " + backupFolder);

                RestoreDescription restoreRescription = new RestoreDescription(backupFolder, RestorePolicy.Force);
                await restoreCtx.RestoreAsync(restoreRescription, cancellationToken);

                ServiceEventSource.Current.ServiceMessage(this.Context, "Restore completed");

                DirectoryInfo tempRestoreDirectory = new DirectoryInfo(backupFolder);
                tempRestoreDirectory.Delete(true);

                return true;
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "Restoration failed: " + "{0} {1}" + e.GetType() + e.Message);

                throw;
            }
        }

        private async Task PeriodicTakeBackupAsync(CancellationToken cancellationToken)
        {
            this.SetupBackupManager();

            cancellationToken.ThrowIfCancellationRequested();

            await Task.Delay(TimeSpan.FromSeconds(this.backupManager.backupFrequencyInSeconds));
            BackupDescription backupDescription = new BackupDescription(BackupOption.Full, this.BackupCallbackAsync);
            await this.BackupAsync(backupDescription, TimeSpan.FromHours(1), cancellationToken);
        }

        private async Task<bool> BackupCallbackAsync(BackupInfo backupInfo, CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(this.Context, "Inside backup callback for replica {0}|{1}", this.Context.PartitionId, this.Context.ReplicaId);
            long totalBackupCount;

            IReliableDictionary<string, long> backupCountDictionary =
                await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>(BackupCountDictionaryName);
            using (ITransaction tx = this.StateManager.CreateTransaction())
            {
                ConditionalValue<long> value = await backupCountDictionary.TryGetValueAsync(tx, "backupCount");

                if (!value.HasValue)
                {
                    totalBackupCount = 0;
                }
                else
                {
                    totalBackupCount = value.Value;
                }

                await backupCountDictionary.SetAsync(tx, "backupCount", ++totalBackupCount);

                await tx.CommitAsync();
            }

            ServiceEventSource.Current.Message("Backup count dictionary updated, total backup count is {0}", totalBackupCount);

            try
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "Archiving backup");
                await this.backupManager.ArchiveBackupAsync(backupInfo, cancellationToken);
                ServiceEventSource.Current.ServiceMessage(this.Context, "Backup archived");
            }
            catch (Exception e)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "Archive of backup failed: Source: {0} Exception: {1}", backupInfo.Directory, e.Message);
            }

            await this.backupManager.DeleteBackupsAsync(cancellationToken);

            ServiceEventSource.Current.Message("Backups deleted");

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
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // create a remote connecting proxy to actor of which backup needs to be taken
            IBackupActorService myActorServiceProxy = ActorServiceProxy.Create<IBackupActorService>(
                new Uri("fabric:/ServiceFabricBackupRestore/BackupActorService"), ActorId.CreateRandom());

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // call to take stateful service backup
                await PeriodicTakeBackupAsync(cancellationToken);

                // call to take actor backup
                await myActorServiceProxy.PeriodicTakeBackupAsync();
                using (var tx = this.StateManager.CreateTransaction())
                {
                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
