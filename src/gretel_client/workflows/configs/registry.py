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
    EvaluateSsDataset = tasks.EvaluateSsDataset
    Holdout = tasks.Holdout
    GenerateColumnConfigFromInstruction = tasks.GenerateColumnConfigFromInstruction
    SampleFromDataset = tasks.SampleFromDataset
    JudgeWithLlm = tasks.JudgeWithLlm
    GenerateColumnsUsingSamplers = tasks.GenerateColumnsUsingSamplers
    ValidateCode = tasks.ValidateCode
    SeedFromRecords = tasks.SeedFromRecords
    EvaluateDataset = tasks.EvaluateDataset
    GenerateColumnFromTemplate = tasks.GenerateColumnFromTemplate
    GenerateSeedCategoryValues = tasks.GenerateSeedCategoryValues
    TabularGan = tasks.TabularGan
    TabularFt = tasks.TabularFt
    Transform = tasks.Transform
    GenerateFromTextFt = tasks.GenerateFromTextFt
    TextFt = tasks.TextFt
    TrainTextFt = tasks.TrainTextFt
    PromptPretrainedModel = tasks.PromptPretrainedModel

