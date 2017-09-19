using BackupActor.Interfaces;
using Microsoft.ServiceFabric.Actors.Runtime;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
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
            : base(context, actorTypeInfo, null, null, new KvsActorStateProvider(false)) // set true to enable incremental backup
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
            this.SetupBackupManager();

            try
            {
                string backupFolder;
                backupFolder = await this.backupManager.RestoreLatestBackupToTempLocation(cancellationToken);
                RestoreDescription restoreRescription = new RestoreDescription(backupFolder, RestorePolicy.Force);

                await restoreCtx.RestoreAsync(restoreRescription, cancellationToken);

                DirectoryInfo tempRestoreDirectory = new DirectoryInfo(backupFolder);
                tempRestoreDirectory.Delete(true);

                return true;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public async Task PeriodicTakeBackupAsync()
        {
            long backupsTaken = 0;
            this.SetupBackupManager();

            await Task.Delay(TimeSpan.FromSeconds(this.backupManager.backupFrequencyInSeconds));
            BackupDescription backupDescription = new BackupDescription(BackupOption.Full, this.BackupCallbackAsync);
            await this.BackupAsync(backupDescription);

            backupsTaken++;
        }

        private async Task<bool> BackupCallbackAsync(BackupInfo backupInfo, CancellationToken cancellationToken)
        {
            try
            {
                await this.backupManager.ArchiveBackupAsync(backupInfo, cancellationToken);
            }
            catch (Exception e)
            {
                // ServiceEventSource.Current.ServiceMessage(this.Context, "Archive of backup failed: Source: {0} Exception: {1}", backupInfo.Directory, e.Message);
            }

            await this.backupManager.DeleteBackupsAsync(cancellationToken);
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
