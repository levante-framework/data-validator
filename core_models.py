from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class Class(BaseModel):
    id: str
    districtId: str
    schoolId: str
    name: str
    clever: bool
    sectionNumber: str = Field(default_factory=lambda: 'Unknown')
    subject: str
    grade: str
    created: datetime
    lastModified: datetime


class School(BaseModel):
    id: str
    districtId: str
    stateId: str = Field(default_factory=lambda: 'Unknown')
    schoolNumber: str
    name: str
    phone: str = Field(default_factory=lambda: 'Unknown')
    clever: bool
    highGrade: str = Field(default_factory=lambda: 'Unknown')
    lowGrade: str = Field(default_factory=lambda: 'Unknown')
    location: dict = {}
    principal: dict = {}
    classes: List[str] = []
    created: datetime
    lastModified: datetime


class District(BaseModel):
    id: str
    name: str
    portalUrl: str
    clever: bool
    districtContact: str
    sis_Type: str
    schools: List[str]
    lastSync: str
    launchDate: datetime
