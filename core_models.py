from pydantic import BaseModel, Field, model_validator
from typing import Optional, List
from datetime import datetime


class Class(BaseModel):
    id: str
    district_id: str
    school_id: str
    name: str
    section_number: Optional[str] = None
    subject: Optional[str] = None
    grade: Optional[str] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class School(BaseModel):
    id: str
    district_id: str
    state_id: Optional[str] = None
    school_number: str
    name: str
    phone: Optional[str] = None
    high_grade: Optional[str] = None
    low_grade: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    principal_name: Optional[str] = None
    principal_email: Optional[str] = None
    created: Optional[datetime] = None
    lastModified: Optional[datetime] = None


class District(BaseModel):
    id: str
    name: str
    portal_url: str
    district_contact_name: Optional[str] = None
    district_contact_email: Optional[str] = None
    district_contact_title: Optional[str] = None
    last_sync: Optional[datetime] = None
    launch_date: Optional[datetime] = None


class Variant(BaseModel):
    id: str
    task_id: str
    name: str = None
    lastUpdated: Optional[datetime] = None


class VariantParams(BaseModel):
    variant_id: str
    params_field: str
    params_type: str
    params_value: str


class Task(BaseModel):
    id: str
    name: str
    registered: bool = None
    description: str = None
    image: str = None
    last_updated: datetime


class Assignment(BaseModel):
    id: str
    name: str
    is_sequential: bool
    created_by: str
    date_created: datetime
    date_closed: datetime
    date_opened: datetime


class AssignmentTask(BaseModel):
    id: int
    assignment_id: str
    task_id: str


class Run(BaseModel):
    id: str
    user_id: str
    task_id: str
    variant_id: str
    assigment_id: str
    completed: bool
    roarScore: str
    score_composite: str
    spr_percentile: str
    spr_standard_score: str
    standard_score: str
    timeFinished: datetime
    timeStarted: datetime


class Trial(BaseModel):
    id: str
    run_id: str
    task_id: str
    subtask_id: str
    assessment_stage: str
    internal_node_id: str
    difficulty: str
    trial_type: str
    corpus_id: str
    correct: bool
    response: str
    stimulus: str
    rt: int
    server_timestamp: datetime


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
    userType: str

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
        return self
