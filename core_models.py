from pydantic import BaseModel, Field, model_validator
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
    districtContact: str
    sis_Type: str
    schools: List[str]
    lastSync: datetime
    launchDate: datetime


class Variant(BaseModel):
    id: str
    name: str = None
    params: dict
    lastUpdated: datetime = None


class Task(BaseModel):
    id: str
    name: str = None
    registered: bool = None
    lastUpdated: datetime
    description: str = None
    image: str = None
    variants: List[Variant]


class Assignment(BaseModel):
    id: str
    started: bool
    dateAssigned: datetime
    dateClosed: datetime
    dateOpened: datetime
    completed: bool
    assessments: List[dict]


class User(BaseModel):
    id: str
    assessmentPid: str
    assessmentUid: str
    assignmentsAssigned: dict
    assignmentsStarted: dict
    classes: dict
    districts: dict
    legal: dict
    name: dict
    schools: dict
    sso: str
    studentData: dict
    userType: str
    assignments: List[dict] = []

    fullName: str = None
    dob: datetime
    gender: str
    grade: str
    state_id: str
    raceList: List[str]
    hispanic_ethnicity: str

    @model_validator(mode='after')
    def assign_attributes(self) -> 'User':
        if self.name.get("first", None) and self.name.get("last", None):
            self.fullName = f"{self.name.get("first", None)} {self.name.get("middle", None)}.{self.name.get("last", None)}"
        else:
            raise ValueError("Incomplete Name.")
        self.dob = self.studentData.get("dob")
        return self


class Run(BaseModel):
    id: str
    user_id: str
    task_id: str
    variant_id: str
    scores: dict
    assignment_id: str
    userType: str
    grade: str
    score: int
    timeFinished: datetime
    timeStarted: datetime
