from pydantic import BaseModel, ConfigDict


class ConfigBase(BaseModel):
    """
    Provides a base class for all our workflow configs to inherit from.

    Some of our configs have fields prefixed with "model_" and other
    protected fields, so we need to remove these protected namespace from
    the base model.

    > Before v2.10, Pydantic used ('model_',) as the default value for this
    setting to prevent collisions between model attributes and BaseModel's own
    methods. This was changed in v2.10 given feedback that this restriction was
    limiting in AI and data science contexts, where it is common to have fields
    with names like model_id, model_input, model_output, etc.

    More information here:
        https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict.protected_namespaces

    `extra` is set to "allow" so that we can evolve the server without
    requiring clients to stay in sync
    """

    model_config = ConfigDict(
        protected_namespaces=(),
        extra="allow",
        validate_default=False,
    )
