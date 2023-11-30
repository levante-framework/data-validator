import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from pydantic import ValidationError
import os
import settings
from core_models import Task, Variant, VariantParams, District, School, Class


class FirestoreServices:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', None)
    default_app = None
    db = None

    def __init__(self, app_name, DB_KEY):
        try:
            if "local" in settings.DB_SITE:
                cred = credentials.Certificate(DB_KEY)
            else:
                cred = credentials.ApplicationDefault()
            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name, options={"projectId": self.project_id})
            self.db = firestore.client(self.default_app)
        except Exception as e:
            print(f"Error in FirestoreService init: {e}")

    def get_districts(self):
        result = []
        try:
            docs = self.db.collection('districts').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['portal_url'] = doc_dict.pop('portalUrl', None)

                district_contact = doc_dict.get('districtContact', {})
                district_contact_name = district_contact.get('name', {})
                doc_dict[
                    'district_contact_name'] = f"{district_contact_name.get('first', None)}, {district_contact_name.get('last', None)}"
                doc_dict['district_contact_email'] = district_contact.get('email', None)
                doc_dict['district_contact_title'] = district_contact.get('title', None)
                doc_dict['last_sync'] = doc_dict.pop('lastSync', None)
                doc_dict['launch_date'] = doc_dict.pop('launchDate', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_districts: {e}")
        return result

    def get_classes(self):
        result = []
        try:
            docs = self.db.collection('classes').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['school_id'] = doc_dict.pop('schoolId', None)
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['section_number'] = doc_dict.pop('sectionNumber', None)
                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_classes: {e}")
        return result

    def get_schools(self):
        result = []
        try:
            docs = self.db.collection('schools').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['state_id'] = doc_dict.pop('stateId', None)
                doc_dict['school_number'] = doc_dict.pop('schoolNumber', None)
                doc_dict['high_grade'] = doc_dict.pop('highGrade', None)
                doc_dict['low_grade'] = doc_dict.pop('lowGrade', None)

                location = doc_dict.get('location', {})
                doc_dict['address'] = location.get('address', None)
                doc_dict['city'] = location.get('city', None)
                doc_dict['state'] = location.get('state', None)
                doc_dict['zip'] = location.get('zip', None)

                principal = doc_dict.get('principal', {})
                doc_dict['principal_name'] = principal.get('name', None)
                doc_dict['principal_email'] = principal.get('email', None)

                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_schools: {e}")
        return result

    def get_tasks(self):
        result = []
        try:
            docs = self.db.collection('tasks').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['last_updated'] = doc_dict.pop('lastUpdated', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_tasks: {e}")
        return result

    def get_variants(self, task_id: str):
        result = []
        try:
            docs = self.db.collection('tasks').document(task_id).collection('variants').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['task_id'] = task_id
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_variants: {e}")
        return result

    def get_users(self):
        result = []
        try:
            docs = self.db.collection('users').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['school_id'] = doc_dict.pop('schoolId', None)
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['section_number'] = doc_dict.pop('sectionNumber', None)
                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_users: {e}")
        return result


fs_assessment = FirestoreServices(app_name='assessment_site', DB_KEY=settings.DB_KEY_LOCATION_ASSESSMENT)
fs_admin = FirestoreServices(app_name='admin_site', DB_KEY=settings.DB_KEY_LOCATION_ADMIN)


class NestedEntityController:
    def __init__(self):
        self.valid_tasks = []
        self.invalid_tasks = []
        self.valid_variants = []
        self.invalid_variants = []
        self.valid_variants_params = []
        self.invalid_variants_params = []
        self.valid_users = []
        self.invalid_users = []

        self.set_task_variant()

    def set_task_variant(self):
        tasks = fs_assessment.get_tasks()
        for task in tasks:
            try:
                task = Task(**task)
                self.valid_tasks.append(task)
                variants = fs_assessment.get_variants(task_id=task.id)

                for variant in variants:
                    try:
                        variant_params = variant.get('params', {})
                        for key, value in variant_params.items():
                            try:
                                self.valid_variants_params.append(VariantParams(
                                    variant_id=variant['id'],
                                    params_field=key,
                                    params_type=str(type(value)),
                                    params_value=str(value)))
                            except ValidationError as e:
                                print(f"Validation error for variant_params {key}, {value}: {e}")
                                self.invalid_variants_params.append(variant_params)

                        variant = Variant(**variant)
                        self.valid_variants.append(variant)
                    except ValidationError as e:
                        print(f"Validation error for variant {variant['id']}: {e}")
                        self.invalid_variants.append(variant)

            except ValidationError as e:
                print(f"Validation error for task {task['id']}: {e}")
                self.invalid_tasks.append(task)


class SimpleEntityController:
    def __init__(self):
        self.valid_districts = []
        self.invalid_districts = []
        self.valid_schools = []
        self.invalid_schools = []
        self.valid_classes = []
        self.invalid_classes = []

        self.set_districts()
        self.set_schools()
        self.set_classes()

    def set_districts(self):
        districts = fs_admin.get_districts()
        for district in districts:
            try:
                district = District(**district)
                self.valid_districts.append(district)

            except ValidationError as e:
                print(f"Validation error for task {district['id']}: {e}")
                self.invalid_districts.append(district)

    def set_schools(self):
        schools = fs_admin.get_schools()
        for school in schools:
            try:
                school = School(**school)
                self.valid_schools.append(school)

            except ValidationError as e:
                print(f"Validation error for task {school['id']}: {e}")
                self.invalid_schools.append(school)

    def set_classes(self):
        classes = fs_admin.get_classes()
        for c in classes:
            try:
                c = Class(**c)
                self.valid_classes.append(c)

            except ValidationError as e:
                print(f"Validation error for task {c['name']}: {e}")
                self.invalid_classes.append(c)
