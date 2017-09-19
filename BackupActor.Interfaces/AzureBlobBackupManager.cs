using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Data;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Fabric.Description;
using Microsoft.WindowsAzure.Storage.Auth;
using System.IO;
using System.IO.Compression;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;

namespace BackupActor.Interfaces
{
    public class AzureBlobBackupManager
    {
        private readonly CloudStorageAccount _account;
        private readonly CloudBlobClient cloudBlobClient;
        private CloudBlobContainer backupBlobContainer;
        private int MaxBackupsToKeep;

        private string PartitionTempDirectory;
        private string partitionId;

        public long backupFrequencyInSeconds;
        private long keyMin;
        private long keyMax;

        public AzureBlobBackupManager(string partitionId, long keymin, long keymax, string codePackageTempDirectory)
        {
            this.keyMin = keymin;
            this.keyMax = keymax;
            
            string backupAccountName = "<your_azurestorage_accountname>";
            string backupAccountKey = "<your_azurestorage_accountkey>";
            this.backupFrequencyInSeconds = 10;
            this.MaxBackupsToKeep = 5;
            this.partitionId = partitionId;
            this.PartitionTempDirectory = Path.Combine(codePackageTempDirectory, partitionId);

            this._account = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName="+backupAccountName+";AccountKey="+backupAccountKey+";EndpointSuffix=core.windows.net");

            this.cloudBlobClient = _account.CreateCloudBlobClient();
            this.backupBlobContainer = this.cloudBlobClient.GetContainerReference(this.partitionId);
            this.backupBlobContainer.CreateIfNotExists();
        }
      

        public async Task ArchiveBackupAsync(BackupInfo backupInfo, CancellationToken cancellationToken)
        {         
            string fullArchiveDirectory = Path.Combine(this.PartitionTempDirectory, Guid.NewGuid().ToString("N"));

            DirectoryInfo fullArchiveDirectoryInfo = new DirectoryInfo(fullArchiveDirectory);
            fullArchiveDirectoryInfo.Create();

            string blobName = string.Format("{0}_{1}_{2}_{3}", Guid.NewGuid().ToString("N"), this.keyMin, this.keyMax, "Backup.zip");
            string fullArchivePath = Path.Combine(fullArchiveDirectory, "Backup.zip");

            ZipFile.CreateFromDirectory(backupInfo.Directory, fullArchivePath, CompressionLevel.Fastest, false);

            DirectoryInfo backupDirectory = new DirectoryInfo(backupInfo.Directory);
            backupDirectory.Delete(true);

            CloudBlockBlob blob = this.backupBlobContainer.GetBlockBlobReference(blobName);
            await blob.UploadFromFileAsync(fullArchivePath, CancellationToken.None);

            DirectoryInfo tempDirectory = new DirectoryInfo(fullArchiveDirectory);
            tempDirectory.Delete(true);
        }

        public async Task<string> RestoreLatestBackupToTempLocation(CancellationToken cancellationToken)
        {
            CloudBlockBlob lastBackupBlob = (await this.GetBackupBlobs(true)).First();
        
            string downloadId = Guid.NewGuid().ToString("N");
            string zipPath = Path.Combine(this.PartitionTempDirectory, string.Format("{0}_Backup.zip", downloadId));
            lastBackupBlob.DownloadToFile(zipPath, FileMode.CreateNew);
            string restorePath = Path.Combine(this.PartitionTempDirectory, downloadId);

            ZipFile.ExtractToDirectory(zipPath, restorePath);
            FileInfo zipInfo = new FileInfo(zipPath);
            zipInfo.Delete();
         
            return restorePath;
        }

        public async Task DeleteBackupsAsync(CancellationToken cancellationToken)
        {
            if (this.backupBlobContainer.Exists())
            {               
                IEnumerable<CloudBlockBlob> oldBackups = (await this.GetBackupBlobs(true)).Skip(this.MaxBackupsToKeep);

                foreach (CloudBlockBlob backup in oldBackups)
                {
                    await backup.DeleteAsync(cancellationToken);
                }
            }
        }

        private async Task<IEnumerable<CloudBlockBlob>> GetBackupBlobs(bool sorted)
        {
            IEnumerable<IListBlobItem> blobs = this.backupBlobContainer.ListBlobs();
          
            List<CloudBlockBlob> itemizedBlobs = new List<CloudBlockBlob>();

            foreach (CloudBlockBlob cbb in blobs)
            {
                await cbb.FetchAttributesAsync();
                itemizedBlobs.Add(cbb);
            }

            if (sorted)
            {
                return itemizedBlobs.OrderByDescending(x => x.Properties.LastModified);
            }
            else
            {
                return itemizedBlobs;
            }
        }
    }
}
