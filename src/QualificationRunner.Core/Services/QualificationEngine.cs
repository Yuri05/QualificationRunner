﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using OSPSuite.Core.Qualification;
using OSPSuite.Core.Services;
using QualificationRunner.Core.Assets;
using QualificationRunner.Core.Domain;
using QualificationRunner.Core.RunOptions;
using ILogger = OSPSuite.Core.Services.ILogger;

namespace QualificationRunner.Core.Services
{
   public class QualificationRunResult
   {
      /// <summary>
      ///    Path of the log file associated with the run
      /// </summary>
      public string LogFileFullPath { get; set; }

      /// <summary>
      ///    Path of the config file associated with the rin
      /// </summary>
      public string ConfigFileFullPath { get; set; }

      /// <summary>
      ///    Was the run successful
      /// </summary>
      public bool Success { get; set; }

      public string ProjectId { get; set; }
   }

   public interface IQualificationEngine : IDisposable
   {
      Task<QualificationRunResult> Run(QualifcationConfiguration qualifcationConfiguration, QualificationRunOptions runOptions, CancellationToken cancellationToken);
      Task<QualificationRunResult> Validate(QualifcationConfiguration qualifcationConfiguration, QualificationRunOptions runOptions, CancellationToken cancellationToken);
   }

   public class QualificationEngine : IQualificationEngine
   {
      private readonly ILogger _logger;
      private readonly IStartableProcessFactory _startableProcessFactory;
      private readonly IQualificationRunnerConfiguration _applicationConfiguration;
      private readonly IJsonSerializer _jsonSerializer;
      private readonly ILogWatcherFactory _logWatcherFactory;

      public QualificationEngine(
         ILogger logger,
         IStartableProcessFactory startableProcessFactory,
         IQualificationRunnerConfiguration applicationConfiguration,
         IJsonSerializer jsonSerializer, ILogWatcherFactory logWatcherFactory)
      {
         _logger = logger;
         _startableProcessFactory = startableProcessFactory;
         _applicationConfiguration = applicationConfiguration;
         _jsonSerializer = jsonSerializer;
         _logWatcherFactory = logWatcherFactory;
      }

      public Task<QualificationRunResult> Validate(QualifcationConfiguration qualifcationConfiguration, QualificationRunOptions runOptions, CancellationToken cancellationToken) =>
         execute(qualifcationConfiguration, runOptions, cancellationToken, validate: true);

      public Task<QualificationRunResult> Run(QualifcationConfiguration qualifcationConfiguration, QualificationRunOptions runOptions, CancellationToken cancellationToken) =>
         execute(qualifcationConfiguration, runOptions, cancellationToken, validate: false);

      private async Task<QualificationRunResult> execute(QualifcationConfiguration qualifcationConfiguration, QualificationRunOptions runOptions, CancellationToken cancellationToken, bool validate)
      {
         _logger.AddDebug(Logs.StartingQualificationRunForProject(qualifcationConfiguration.ProjectId));

         var logFile = Path.Combine(qualifcationConfiguration.TempFolder, "log.txt");
         var configFile = Path.Combine(qualifcationConfiguration.TempFolder, "config.json");


         var qualificationRunResult = new QualificationRunResult
         {
            ConfigFileFullPath = configFile,
            LogFileFullPath = logFile,
            ProjectId = qualifcationConfiguration.ProjectId
         };

         await _jsonSerializer.Serialize(qualifcationConfiguration, configFile);

         _logger.AddDebug(Logs.QualificationConfigurationForProjectExportedTo(qualifcationConfiguration.ProjectId, configFile));

         return await Task.Run(() =>
         {
            var code = startBatchProcess(configFile, logFile, runOptions.LogLevel, validate, cancellationToken);
            qualificationRunResult.Success = (code == ExitCodes.Success);
            return qualificationRunResult;
         }, cancellationToken);
      }

      private ExitCodes startBatchProcess(string configFile, string logFile, LogLevel logLevel, bool validate, CancellationToken cancellationToken)
      {
         var args = new List<string>
         {
            "qualification",
            "-f",
            configFile.InQuotes(),
            "-l",
            logFile.InQuotes(),
            "--logLevel",
            logLevel.ToString()
         };

         if (validate)
            args.Add("-v");

         using (var process = _startableProcessFactory.CreateStartableProcess(_applicationConfiguration.PKSimCLIPath, args.ToArray()))
         using (var watcher = _logWatcherFactory.CreateLogWatcher(logFile, new List<string>().ToArray()))
         {
            watcher.Watch();
            process.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            process.Start();
            process.Wait(cancellationToken);
            return (ExitCodes) process.ReturnCode;
         }
      }

      protected virtual void Cleanup()
      {
      }

      #region Disposable properties

      private bool _disposed;

      public void Dispose()
      {
         if (_disposed) return;

         Cleanup();
         GC.SuppressFinalize(this);
         _disposed = true;
      }

      ~QualificationEngine()
      {
         Cleanup();
      }

      #endregion
   }
}