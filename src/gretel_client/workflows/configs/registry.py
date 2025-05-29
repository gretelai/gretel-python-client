# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from typing import Any

from gretel_client.workflows.configs import tasks


class RegistryMeta(type):
    def __setattr__(cls, name: str, value: Any) -> None:
        super().__setattr__(name, value)


class Registry(metaclass=RegistryMeta):
    GenerateDatasetFromSampleRecords = tasks.GenerateDatasetFromSampleRecords
    SampleDataSeeds = tasks.SampleDataSeeds
    EvaluateDataDesignerDataset = tasks.EvaluateDataDesignerDataset
    SampleFromDataset = tasks.SampleFromDataset
    Holdout = tasks.Holdout
    GenerateColumnFromExpression = tasks.GenerateColumnFromExpression
    GenerateSamplingColumnConfigFromInstruction = (
        tasks.GenerateSamplingColumnConfigFromInstruction
    )
    ExtractDataSeedsFromSampleRecords = tasks.ExtractDataSeedsFromSampleRecords
    RunSampleToDataset = tasks.RunSampleToDataset
    GetGretelDataset = tasks.GetGretelDataset
    JudgeWithLlm = tasks.JudgeWithLlm
    IdGenerator = tasks.IdGenerator
    Combiner = tasks.Combiner
    GenerateColumnFromTemplateV2 = tasks.GenerateColumnFromTemplateV2
    DropColumns = tasks.DropColumns
    ConcatDatasets = tasks.ConcatDatasets
    GenerateColumnConfigFromInstruction = tasks.GenerateColumnConfigFromInstruction
    EvaluateSafeSyntheticsDataset = tasks.EvaluateSafeSyntheticsDataset
    DummyTaskWithInputs = tasks.DummyTaskWithInputs
    DummyTaskWithListOfInputs = tasks.DummyTaskWithListOfInputs
    NameGenerator = tasks.NameGenerator
    TestFailingTask = tasks.TestFailingTask
    TestOptionalArgTask = tasks.TestOptionalArgTask
    TestRequiredAndOptionalArgsTask = tasks.TestRequiredAndOptionalArgsTask
    TestTaskCallingTask = tasks.TestTaskCallingTask
    TestUnhandledErrorTask = tasks.TestUnhandledErrorTask
    GenerateColumnsUsingSamplers = tasks.GenerateColumnsUsingSamplers
    LoadDataSeeds = tasks.LoadDataSeeds
    TabularFt = tasks.TabularFt
    PromptPretrainedModel = tasks.PromptPretrainedModel
    TabularGan = tasks.TabularGan
    Transform = tasks.Transform
    TextFt = tasks.TextFt
    ValidateCode = tasks.ValidateCode
    EvaluateDataset = tasks.EvaluateDataset
    SeedFromRecords = tasks.SeedFromRecords
    S3Destination = tasks.S3Destination
    S3Source = tasks.S3Source
    DataSource = tasks.DataSource
    GcsDestination = tasks.GcsDestination
    GcsSource = tasks.GcsSource
    BigqueryDestination = tasks.BigqueryDestination
    BigquerySource = tasks.BigquerySource
    SnowflakeDestination = tasks.SnowflakeDestination
    SnowflakeSource = tasks.SnowflakeSource
    DatabricksDestination = tasks.DatabricksDestination
    DatabricksSource = tasks.DatabricksSource
    PostgresDestination = tasks.PostgresDestination
    PostgresSource = tasks.PostgresSource
    MysqlDestination = tasks.MysqlDestination
    MysqlSource = tasks.MysqlSource
    AzureDestination = tasks.AzureDestination
    AzureSource = tasks.AzureSource
    OracleDestination = tasks.OracleDestination
    OracleSource = tasks.OracleSource
    MssqlDestination = tasks.MssqlDestination
    MssqlSource = tasks.MssqlSource

