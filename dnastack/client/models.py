from typing import Any, Dict, Optional, List, Union

from uuid import uuid4

from pydantic import Field, BaseModel

from dnastack.client.service_registry.models import ServiceType
from dnastack.common.model_mixin import JsonModelMixin as HashableModel


class EndpointSource(BaseModel):
    source_id: str
    """ The ID of the source of the endpoint configuration 
    
        This references an service endpoint in the configuration (file). 
    """

    external_id: str
    """ This endpoint's identifier in the external source system """


class ServiceEndpoint(BaseModel, HashableModel):
    """API Service Endpoint"""
    dnastack_schema_version: float = Field(alias='model_version', default=2.0)
    """ Service Endpoint Configuration Specification Version """

    id: str = Field(default_factory=lambda: str(uuid4()))
    """ Local Unique ID"""

    adapter_type: Optional[str] = None
    """ Adapter type (only used with ClientManager)
    
        DO NOT USE THIS. This is replaced by "type" in model version 2.0.
    """

    authentication: Optional[Dict[str, Any]] = None
    """ (Primary) authentication information """

    fallback_authentications: Optional[List[Dict[str, Any]]] = None
    """ The list of fallback Authentication information
    
        This is in junction with GA4GH Service Information.
    """

    type: Optional[ServiceType]
    """ Service Type """

    url: str
    """ Base URL """

    # DEPRECATED: It is here only for the migration.
    mode: Optional[str]
    """ Client mode ("standard" or "explorer") - only applicable if the client supports.
    
        DO NOT USE THIS. This is replaced by "type" in model version 2.0.
    """

    # DEPRECATED: It is here only for the migration.
    source: Optional[EndpointSource]
    """ The source of the endpoint configuration (e.g., service registry) """

    def get_authentications(self) -> List[Dict[str, Any]]:
        """ Get the list of authentication information """
        raw_auths = []

        if self.authentication:
            raw_auths.append(self.authentication)
        if self.fallback_authentications:
            raw_auths.extend(self.fallback_authentications)

        return [self.__convert_to_dict(raw_auth) for raw_auth in raw_auths]

    def __convert_to_dict(self, model: Union[Dict[str, Any], BaseModel]) -> Dict[str, Any]:
        converted_model: Dict[str, Any] = dict()

        if isinstance(model, dict):
            converted_model.update(model)
        elif isinstance(model, BaseModel):
            converted_model.update(model.dict())
        else:
            raise NotImplementedError(f'No interpretation for {model}')

        # Short-term backward-compatibility until May 2022
        if 'oauth2' in converted_model:
            converted_model = converted_model['oauth2']
            converted_model['type'] = 'oauth2'

        return converted_model