# AUTO-GENERATED FILE, DO NOT EDIT DIRECTLY
import inspect

from typing import Any, cast, Type, TypeVar

from gretel_client.workflows.configs import tasks


class RegistryMeta(type):
    def __setattr__(cls, name: str, value: Any) -> None:
        super().__setattr__(name, value)


class Registry(metaclass=RegistryMeta):

    ExtractDataSeedsFromSampleRecords = tasks.ExtractDataSeedsFromSampleRecords
    IdGenerator = tasks.IdGenerator
    LoadDataSeeds = tasks.LoadDataSeeds
    NameGenerator = tasks.NameGenerator
    GenerateDatasetFromSampleRecords = tasks.GenerateDatasetFromSampleRecords
    SampleDataSeeds = tasks.SampleDataSeeds
    GetGretelDataset = tasks.GetGretelDataset
    Combiner = tasks.Combiner
    DummyTaskWithInputs = tasks.DummyTaskWithInputs
    DummyTaskWithListOfInputs = tasks.DummyTaskWithListOfInputs
    TestFailingTask = tasks.TestFailingTask
    TestOptionalArgTask = tasks.TestOptionalArgTask
    TestRequiredAndOptionalArgsTask = tasks.TestRequiredAndOptionalArgsTask
    TestTaskCallingTask = tasks.TestTaskCallingTask
    Holdout = tasks.Holdout
    JudgeWithLlm = tasks.JudgeWithLlm
    GenerateColumnsUsingSamplers = tasks.GenerateColumnsUsingSamplers
    ValidateCode = tasks.ValidateCode
    SeedFromRecords = tasks.SeedFromRecords
    EvaluateDataset = tasks.EvaluateDataset
    GenerateColumnFromTemplate = tasks.GenerateColumnFromTemplate
    GenerateSeedCategoryValues = tasks.GenerateSeedCategoryValues
    TabularGan = tasks.TabularGan
    TabularFt = tasks.TabularFt
    GenerateFromTextFt = tasks.GenerateFromTextFt
    TextFt = tasks.TextFt
    TrainTextFt = tasks.TrainTextFt
    PromptPretrainedModel = tasks.PromptPretrainedModel


