import logging

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from gretel_client.config import ClientConfig
from gretel_client.navigator.client.utils import get_navigator_client
from gretel_client.navigator.data_designer.interface import DataDesigner
from gretel_client.navigator.data_designer.sample_to_dataset import (
    DataDesignerFromSampleRecords,
)
from gretel_client.navigator.log import get_logger
from gretel_client.navigator.tasks.types import (
    DEFAULT_MODEL_SUITE,
    ModelSuite,
    RecordsT,
)

logger = get_logger(__name__, level=logging.INFO)


class DataDesignerFactory:
    """Factory class for creating DataDesigner instances.

    Each class method on this object provides a different way to instantiate
    a DataDesigner object, depending on your use case and desired workflow.

    Allowed session keyword arguments:
        api_key (str): Your Gretel API key. If set to "prompt" and no API key
            is found on the system, you will be prompted for the key.
        endpoint (str): Specifies the Gretel API endpoint. This must be a fully
            qualified URL. The default is "https://api.gretel.cloud".
        default_runner (str): Specifies the runner mode. Must be one of "cloud",
            "local", "manual", or "hybrid". The default is "cloud".
        artifact_endpoint (str): Specifies the endpoint for project and model
            artifacts. Defaults to "cloud" for running in Gretel Cloud. If
            working in hybrid mode, set to the URL of your artifact storage bucket.
        cache (str): Valid options are "yes" or "no". If set to "no", the session
            configuration will not be written to disk. If set to "yes", the
            session configuration will be written to disk only if one doesn't
            already exist. The default is "no".
        validate (bool): If `True`, will validate the login credentials at
            instantiation. The default is `False`.
        clear (bool): If `True`, existing Gretel credentials will be removed.
            The default is `False.`
    """

    @classmethod
    def from_blank_canvas(
        cls,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> DataDesigner:
        """Instantiate an empty DataDesigner instance that can be built up programmatically.

        This initialization method is equivalent to directly instantiating a DataDesigner object.

        Args:
            model_suite: The model suite to use for generating synthetic data. Defaults to the
                apache-2.0 licensed model suite.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.

        Returns:
            An instance of DataDesigner with a blank canvas.
        """
        logger.info("🎨 Creating DataDesigner instance from blank canvas")
        return DataDesigner(model_suite=model_suite, session=session, **session_kwargs)

    @classmethod
    def from_config(
        cls,
        config: Union[dict, str, Path],
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> DataDesigner:
        """Instantiate a DataDesigner instance from a configuration dictionary.

        This method allows you to specify your data design using a YAML configuration file,
        which is then built into a DataDesigner instance the same way you would do so programmatically.

        Args:
            config: A YAML configuration file, dict, or string that fully specifies the data design.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.

        Returns:
            An instance of DataDesigner configured with the data seeds and generated data columns
            defined in the configuration dictionary.
        """
        logger.info("🎨 Creating DataDesigner instance from config")
        return DataDesigner.from_config(config, session=session, **session_kwargs)

    @classmethod
    def from_sample_records(
        cls,
        sample_records: Union[str, Path, pd.DataFrame, RecordsT],
        *,
        subsample_size: Optional[int] = None,
        model_suite: ModelSuite = DEFAULT_MODEL_SUITE,
        session: Optional[ClientConfig] = None,
        **session_kwargs,
    ) -> DataDesigner:
        """Instantiate a DataDesigner instance from sample records.

        Use this subclass of DataDesigner when you want to turn a few sample records
        into a rich, diverse synthetic dataset (Sample-to-Dataset).

        Args:
            sample_records: Sample records from which categorical data seeds will be extracted
                and optionally used to create generated data columns.
            subsample_size: The number of records to use from the sample records. If None,
                all records will be used. If the subsample size is larger than the sample records,
                the full sample will be used.
            model_suite: The model suite to use for generating synthetic data. Defaults to the
                apache-2.0 licensed model suite.
            session: Optional Gretel session configuration object. If not provided, the session
                will be configured based on the provided session_kwargs or cached session
                configuration.
            **session_kwargs: kwargs for your Gretel session. See options in the class
                docstring above.
        Returns:
            An instance of DataDesigner configured to extract data seeds from the sample records
            and optionally create generated data columns for each field in the sample records.
        """
        logger.info("🎨 Creating DataDesigner instance from sample records")

        return DataDesignerFromSampleRecords(
            sample_records=sample_records,
            subsample_size=subsample_size,
            model_suite=model_suite,
            session=session,
            **session_kwargs,
        )
