# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY

from typing import Any

from gretel_client.workflows.configs import tasks


class RegistryMeta(type):
    def __setattr__(cls, name: str, value: Any) -> None:
        super().__setattr__(name, value)


class Registry(metaclass=RegistryMeta):

    ConcatDatasets = tasks.ConcatDatasets
    ExtractDataSeedsFromSampleRecords = tasks.ExtractDataSeedsFromSampleRecords
    IdGenerator = tasks.IdGenerator
    LoadDataSeeds = tasks.LoadDataSeeds
    DropColumns = tasks.DropColumns
    NameGenerator = tasks.NameGenerator
    EvaluateDdDataset = tasks.EvaluateDdDataset
    GenerateDatasetFromSampleRecords = tasks.GenerateDatasetFromSampleRecords
    SampleDataSeeds = tasks.SampleDataSeeds
    GetGretelDataset = tasks.GetGretelDataset
    GenerateColumnFromExpression = tasks.GenerateColumnFromExpression
    Combiner = tasks.Combiner
    DummyTaskWithInputs = tasks.DummyTaskWithInputs
    DummyTaskWithListOfInputs = tasks.DummyTaskWithListOfInputs
    TestFailingTask = tasks.TestFailingTask
    TestOptionalArgTask = tasks.TestOptionalArgTask
    TestRequiredAndOptionalArgsTask = tasks.TestRequiredAndOptionalArgsTask
    TestTaskCallingTask = tasks.TestTaskCallingTask
    TestUnhandledErrorTask = tasks.TestUnhandledErrorTask
    GenerateSamplingColumnConfigFromInstruction = (
        tasks.GenerateSamplingColumnConfigFromInstruction
    )
    Holdout = tasks.Holdout
    GenerateColumnConfigFromInstruction = tasks.GenerateColumnConfigFromInstruction
    SampleFromDataset = tasks.SampleFromDataset
    JudgeWithLlm = tasks.JudgeWithLlm
    GenerateColumnsUsingSamplers = tasks.GenerateColumnsUsingSamplers
    EvaluateSafeSyntheticsDataset = tasks.EvaluateSafeSyntheticsDataset
    ValidateCode = tasks.ValidateCode
    SeedFromRecords = tasks.SeedFromRecords
    EvaluateDataset = tasks.EvaluateDataset
    GenerateColumnFromTemplate = tasks.GenerateColumnFromTemplate
    GenerateSeedCategoryValues = tasks.GenerateSeedCategoryValues
    TabularGan = tasks.TabularGan
    TabularFt = tasks.TabularFt
    Transform = tasks.Transform
    TextFt = tasks.TextFt
    PromptPretrainedModel = tasks.PromptPretrainedModel
    DataSource = tasks.DataSource
    AzureDestination = tasks.AzureDestination
    AzureSource = tasks.AzureSource
    MssqlDestination = tasks.MssqlDestination
    MssqlSource = tasks.MssqlSource
    GcsDestination = tasks.GcsDestination
    GcsSource = tasks.GcsSource
    BigqueryDestination = tasks.BigqueryDestination
    BigquerySource = tasks.BigquerySource
    SnowflakeDestination = tasks.SnowflakeDestination
    SnowflakeSource = tasks.SnowflakeSource
    PostgresDestination = tasks.PostgresDestination
    PostgresSource = tasks.PostgresSource
    DatabricksDestination = tasks.DatabricksDestination
    DatabricksSource = tasks.DatabricksSource
    OracleDestination = tasks.OracleDestination
    OracleSource = tasks.OracleSource
    S3Destination = tasks.S3Destination
    S3Source = tasks.S3Source
    MysqlDestination = tasks.MysqlDestination
    MysqlSource = tasks.MysqlSource

