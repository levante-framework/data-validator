import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from pydantic import ValidationError
import os
import settings
from core_models import Task, Variant, VariantParams, District, School, Class, User, UserClass, UserAssignment, Assignment, AssignmentTask


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
            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name,
                                                             options={"projectId": self.project_id})
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
                doc_dict['id'] = doc.id
                doc_dict['assessment_pid'] = doc_dict.pop('assessmentPid', None)
                doc_dict['assessment_uid'] = doc_dict.pop('assessmentUid', None)
                doc_dict['user_type'] = doc_dict.pop('userType', None)

                name = doc_dict.get('name', {})
                doc_dict['full_name'] = f"{name.get('first', '')} {name.get('middle', '')}. {name.get('last', '')}"

                student_data = doc_dict.get('studentData', {})
                doc_dict['dob'] = student_data.get('dob', None)
                doc_dict['gender'] = student_data.get('gender', None)
                doc_dict['grade'] = student_data.get('grade', None)
                doc_dict['hispanic_ethnicity'] = student_data.get('hispanic_ethnicity', None)
                doc_dict['state_id'] = student_data.get('state_id', None)
                doc_dict['races'] = ', '.join(student_data.get('race', []))

                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_users: {e}")
        return result

    def get_assignments(self):
        result = []
        try:
            docs = self.db.collection('administrations').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['is_sequential'] = doc_dict.pop('sequential', None)
                doc_dict['created_by'] = doc_dict.pop('createdBy', None)
                doc_dict['date_created'] = doc_dict.pop('dateCreated', None)
                doc_dict['date_closed'] = doc_dict.pop('dateClosed', None)
                doc_dict['date_opened'] = doc_dict.pop('dateOpened', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_assignments: {e}")
        return result

    def get_runs(self):
        pass

    def get_trails(self, run_id: str):
        pass


fs_assessment = FirestoreServices(app_name='assessment_site', DB_KEY=settings.DB_KEY_LOCATION_ASSESSMENT)
fs_admin = FirestoreServices(app_name='admin_site', DB_KEY=settings.DB_KEY_LOCATION_ADMIN)


class EntityController:
    def __init__(self):
        self.valid_tasks = []
        self.invalid_tasks = []
        self.valid_variants = []
        self.invalid_variants = []
        self.valid_variants_params = []
        self.invalid_variants_params = []
        self.set_task()

        self.valid_districts = []
        self.invalid_districts = []
        self.valid_schools = []
        self.invalid_schools = []
        self.valid_classes = []
        self.invalid_classes = []
        self.set_districts()
        self.set_schools()
        self.set_classes()

        self.valid_users = []
        self.invalid_users = []
        self.valid_user_class = []
        self.invalid_user_class = []
        self.valid_user_assignment = []
        self.invalid_user_assignment = []
        self.set_users()

        self.valid_assignments = []
        self.invalid_assignments = []
        self.valid_assignment_task = []
        self.invalid_assignment_task = []
        self.set_assignments()

    def set_districts(self):
        districts = fs_admin.get_districts()
        for district in districts:
            try:
                district = District(**district)
                self.valid_districts.append(district)

            except ValidationError as e:
                print(f"Validation error for district {district['id']}: {e}")
                self.invalid_districts.append(district)

    def set_schools(self):
        schools = fs_admin.get_schools()
        for school in schools:
            try:
                school = School(**school)
                self.valid_schools.append(school)

            except ValidationError as e:
                print(f"Validation error for school {school['id']}: {e}")
                self.invalid_schools.append(school)

    def set_classes(self):
        classes = fs_admin.get_classes()
        for c in classes:
            try:
                c = Class(**c)
                self.valid_classes.append(c)

            except ValidationError as e:
                print(f"Validation error for class {c['name']}: {e}")
                self.invalid_classes.append(c)

    def set_task(self):
        tasks = fs_assessment.get_tasks()
        for task in tasks:
            try:
                task = Task(**task)
                self.valid_tasks.append(task)
                self.set_variant(task.id)
            except ValidationError as e:
                print(f"Validation error for task {task['id']}: {e}")
                self.invalid_tasks.append(task)

    def set_variant(self, task_id: str):
        variants = fs_assessment.get_variants(task_id=task_id)
        for variant in variants:
            try:
                self.set_variant_params(variant)
                variant = Variant(**variant)
                self.valid_variants.append(variant)
            except ValidationError as e:
                print(f"Validation error for variant {variant['id']}: {e}")
                self.invalid_variants.append(variant)

    def set_variant_params(self, variant: dict):
        variant_id = variant.get('id', None)
        variant_params = variant.get('params', {})
        for key, value in variant_params.items():
            try:
                self.valid_variants_params.append(VariantParams(
                    variant_id=variant_id,
                    params_field=key,
                    params_type=str(type(value)),
                    params_value=str(value)))
            except ValidationError as e:
                print(f"Validation error for variant: {variant_id}, variant_params {key}, {value}: {e}")
                self.invalid_variants_params.append(variant_id)

    def set_users(self):
        users = fs_admin.get_users()
        for user in users:
            try:
                self.set_user_class(user)
                self.set_user_assignment(user)
                user = User(**user)
                self.valid_users.append(user)
            except ValidationError as e:
                print(f"Validation error for user {user['id']}: {e}")
                self.invalid_users.append(user)

    def set_user_class(self, user: dict):
        user_id = user.get('id', None)
        variant_classes = user.get('classes', {})
        all_classes = variant_classes.get('all', [])
        current_classes = variant_classes.get('current', [])
        for class_id in all_classes:
            try:
                self.valid_user_class.append(UserClass(
                    user_id=user_id,
                    class_id=class_id,
                    is_active=True if class_id in current_classes else False))
            except ValidationError as e:
                print(f"Validation error for user: {user_id}, class: {class_id}, {e}")
                self.invalid_user_class.append(f"{user_id},{class_id}")

    def set_user_assignment(self, user: dict):
        user_id = user.get('id', None)
        assignments_assigned = user.get('assignmentsAssigned', {})
        assignments_started = user.get('assignmentsStarted', {})
        for key, value in assignments_assigned.items():
            try:
                self.valid_user_assignment.append(UserAssignment(
                    user_id=user_id,
                    assignment_id=key,
                    is_started=True if key in assignments_started.keys() else False,
                    date_time=value))
            except ValidationError as e:
                print(f"Validation error for user: {user_id}, assignment: {key}, {e}")
                self.invalid_user_assignment.append(f"{user_id},{key}")

    def set_assignments(self):
        assignments = fs_admin.get_assignments()
        for assignment in assignments:
            try:
                self.set_assignment_task(assignment)
                assignment = Assignment(**assignment)
                self.valid_assignments.append(assignment)
            except ValidationError as e:
                print(f"Validation error for task {assignment['id']}: {e}")
                self.invalid_assignments.append(assignment)

    def set_assignment_task(self, assignment: dict):
        assignment_id = assignment.get('id', None)
        tasks = assignment.get('assessments', [])
        for task in tasks:
            task_id = task.get('taskId', None)
            try:
                self.valid_assignment_task.append(AssignmentTask(
                    assignment_id=assignment_id,
                    task_id=task_id))
            except ValidationError as e:
                print(f"Validation error for assignment {assignment_id}, task {task_id}: {e}")
                self.invalid_assignments.append(f"{assignment_id},{task_id}")

    def set_runs(self):
        pass

    def set_trails(self):
        pass

    def set_score_details(self):
        pass