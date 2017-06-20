using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using System.IO;
using BackupActor.Interfaces;

namespace ReliableStatefulBackupService
{    
    internal sealed class ReliableStatefulBackupService : StatefulService
    {
        private AzureBlobBackupManager backupManager;
        private const string BackupCountDictionaryName = "BackupCountingDictionary";
        public ReliableStatefulBackupService(StatefulServiceContext context)
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
            long backupsTaken = 0;
            this.SetupBackupManager();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await Task.Delay(TimeSpan.FromSeconds(this.backupManager.backupFrequencyInSeconds));

                BackupDescription backupDescription;

               // if (backupsTaken==0)
                    backupDescription = new BackupDescription(BackupOption.Full, this.BackupCallbackAsync);
              //  else
              //      backupDescription = new BackupDescription(BackupOption.Incremental, this.BackupCallbackAsync);

                await this.BackupAsync(backupDescription, TimeSpan.FromHours(1), cancellationToken);

                backupsTaken++;

                ServiceEventSource.Current.ServiceMessage(this.Context, "Backup {0} taken", backupsTaken);

            }
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
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            //IBackupActorService myActorServiceProxy = ActorServiceProxy.Create<IBackupActorService>(
            // new Uri("fabric:/ServiceFabricBackupRestore/BackupActorService"), ActorId.CreateRandom());

            //await myActorServiceProxy.PeriodicTakeBackupAsync();

            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");
            await PeriodicTakeBackupAsync(cancellationToken);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                    ServiceEventSource.Current.ServiceMessage(this.Context, "Current Counter Value: {0}",
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
