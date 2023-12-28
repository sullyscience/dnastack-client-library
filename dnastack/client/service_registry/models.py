from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ServiceType(BaseModel):
    """
    GA4GH Service Type

    https://raw.githubusercontent.com/ga4gh-discovery/ga4gh-service-info/v1.0.0/service-info.yaml#/components/schemas/ServiceType
    """
    group: str
    artifact: str
    version: str

    def __repr__(self):
        return f'{self.group}:{self.artifact}:{self.version}'

    def __str__(self):
        return f'{self.group}:{self.artifact}:{self.version}'


class Organization(BaseModel):
    """ Organization """
    name: str
    url: str


class Service(BaseModel):
    """
    GA4GH Service

    * https://github.com/ga4gh-discovery/ga4gh-service-registry/blob/develop/service-registry.yaml#/components/schemas/ExternalService
    * https://raw.githubusercontent.com/ga4gh-discovery/ga4gh-service-info/v1.0.0/service-info.yaml#/components/schemas/Service
    """
    id: str
    name: str
    type: ServiceType
    url: Optional[str] = None
    description: Optional[str] = None
    organization: Optional[Organization] = None
    contactUrl: Optional[str] = None
    documentationUrl: Optional[str] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None
    environment: Optional[str] = None
    version: Optional[str] = None

    authentication: Optional[List[Dict[str, Any]]] = None
    """
    Authentication Information
    
    .. note:: This is a non-standard property. Only available via DNAstack's GA4GH Service Registry.
    """
