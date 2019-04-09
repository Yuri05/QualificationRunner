﻿using System.Collections.Generic;
using System.Linq;
using OSPSuite.Core.Domain;
using OSPSuite.Utility.Validation;

namespace OSPSuite.Core.Qualification
{
   public interface IReferencingProject
   {
      string Project { get; set; }
   }

   public interface IReferencingSimulation : IReferencingProject
   {
      string Simulation { get; set; }
   }

   public static class QualificationExtensions
   {
      public static T[] ForProject<T>(this IEnumerable<T> referencingProject, string projectName) where T : IReferencingProject =>
         referencingProject?.Where(x => string.Equals(x.Project, projectName)).ToArray();
   }

   public class BuildingBlockSwap : IWithName
   {
      public PKSimBuildingBlockType Type { get; set; }
      public string Name { get; set; }
      public string SnapshotFile { get; set; }

      public void Deconstruct(out PKSimBuildingBlockType type, out string name, out string snapshotFile)
      {
         type = Type;
         name = Name;
         snapshotFile = SnapshotFile;
      }
   }

   public class BuildingBlockRef : IWithName, IReferencingProject
   {
      public PKSimBuildingBlockType Type { get; set; }
      public string Name { get; set; }
      public string Project { get; set; }
   }

   public class SimulationParameterSwap
   {
      public string SnapshotFile { get; set; }
      public string Simulation { get; set; }
      public string ParameterPath { get; set; }
      public string[] TargetSimulations { get; set; }
   }

   public class SimulationParameterRef : IReferencingSimulation
   {
      public string Path { get; set; }
      public string[] TargetSimulations { get; set; }
      public string Project { get; set; }
      public string Simulation { get; set; }
   }

   public class Input : BuildingBlockRef
   {
      public int SectionId { get; set; }
   }

   public class SimulationPlot : IReferencingSimulation
   {
      public string Simulation { get; set; }
      public string Project { get; set; }
      public int SectionId { get; set; }
   }

   public class QualifcationConfiguration : IValidatable, IReferencingProject
   {
      /// <summary>
      ///    Typically name of the project as referenced in the configuration fie
      /// </summary>
      public string Project { get; set; }

      /// <summary>
      ///    Path of project snapshot file used for this qualificaiton run
      /// </summary>
      public string SnapshotFile { get; set; }

      /// <summary>
      ///    Output folder where project artefacts will be exported. It will be created if it does not exist
      /// </summary>
      public string OutputFolder { get; set; }

      /// <summary>
      ///    Folder were observed data will be exported
      /// </summary>
      public string ObservedDataFolder { get; set; }

      /// <summary>
      ///    Folder were inputs will be exported
      /// </summary>
      public string InputsFolder { get; set; }

      /// <summary>
      ///    Path of mapping file that will be created for the project.
      /// </summary>
      public string MappingFile { get; set; }

      /// <summary>
      ///    Temp folder where potential artefacts can be exported
      /// </summary>
      public string TempFolder { get; set; }

      /// <summary>
      ///    Path of configuration file that will be created as part of the qualificaton run
      /// </summary>
      public string ReportConfigurationFile { get; set; }

      public SimulationPlot[] SimulationPlots { get; set; }

      public Input[] Inputs { get; set; }

      public BuildingBlockSwap[] BuildingBlocks { get; set; }

      public SimulationParameterSwap[] SimulationParameters { get; set; }

      public IBusinessRuleSet Rules { get; } = new BusinessRuleSet();

//      public QualifcationConfiguration()
//      {
//         Rules.AddRange(new[]
//         {
//            GenericRules.FileExists<QualifcationConfiguration>(x => x.SnapshotFile),
//            GenericRules.NonEmptyRule<QualifcationConfiguration>(x => x.OutputFolder, QualificationOutputFolderNotDefined),
//            GenericRules.NonEmptyRule<QualifcationConfiguration>(x => x.MappingFile, QualificationMappingFileNotDefined),
//            GenericRules.NonEmptyRule<QualifcationConfiguration>(x => x.ReportConfigurationFile, QualificationReportConfigurationFileNotDefined),
//            GenericRules.NonEmptyRule<QualifcationConfiguration>(x => x.ObservedDataFolder, QualificationObservedDataFolderNotDefined)
//         });
//      }
   }
}