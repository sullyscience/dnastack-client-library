from uuid import uuid4

from pydantic import BaseModel, Field
from typing import List, Optional, Dict

from dnastack.client.models import ServiceEndpoint as Endpoint
from dnastack.context.models import Context

DEFAULT_CONTEXT = 'default'


class Configuration(BaseModel):
    """
    Configuration

    Please note that "defaults" and "endpoints" are for backward compatibility. They are ignored in version 4 onward.
    """
    version: float = 4

    # For debugging
    guid: Optional[str] = Field(default_factory=lambda: str(uuid4()))

    #############
    # Version 4 #
    #############
    current_context: Optional[str] = DEFAULT_CONTEXT
    contexts: Dict[str, Context] = Field(default_factory=lambda: {DEFAULT_CONTEXT: Context()})

    ###############################################################
    # Version 3 (for object migration and backward compatibility) #
    ###############################################################
    defaults: Optional[Dict[str, str]]
    endpoints: Optional[List[Endpoint]]
