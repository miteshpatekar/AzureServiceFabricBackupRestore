using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Services.Remoting;

namespace BackupActor.Interfaces
{
    public interface IBackupActorService : IService
    {
        Task PeriodicTakeBackupAsync();
    }
}
