import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from pydantic import ValidationError
import os
import settings
from google.cloud import storage
from google.oauth2 import service_account
from core_models import Task, Variant, VariantParams, District, School, Class, User, UserClass, UserAssignment, \
    Assignment, AssignmentTask, Run, ScoreDetails, Trial


def upload_blob_from_memory(bucket_name, data, destination_blob_name, content_type):
    """
        Uploads a file from memory to Google Cloud Storage.

        Args:
        - bucket_name (str): Name of the GCS bucket.
        - data (bytes or str): Data to upload.
        - destination_blob_name (str): Desired name for the file in the bucket.
        - content_type (str): Content type of the file (e.g., 'application/json', 'text/csv').
        """
    # Create a client
    if "local" in settings.DB_SITE:
        cred = service_account.Credentials.from_service_account_file(filename=settings.SA_KEY_LOCATION_ADMIN)
        storage_client = storage.Client(credentials=cred)
    else:
        storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    try:
        blob.upload_from_string(data, content_type=content_type)
        print(f"Data uploaded to {destination_blob_name}.")
    except Exception as e:
        print(f"Failed to upload to {destination_blob_name}, {e}")


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

    def get_districts(self, lab_id: str):
        result = []
        try:
            doc = self.db.collection('districts').document(lab_id).get()
            doc_dict = doc.to_dict()  # Convert the document to a dictionary
            doc_dict['portal_url'] = doc_dict.pop('portalUrl', None)

            district_contact = doc_dict.get('districtContact', {})
            district_contact_name = district_contact.get('name', {})
            doc_dict[
                'district_contact_name'] = f"{district_contact_name.get('first', '')}, {district_contact_name.get('last', '')}"
            doc_dict['district_contact_email'] = district_contact.get('email', None)
            doc_dict['district_contact_title'] = district_contact.get('title', None)
            doc_dict['last_sync'] = doc_dict.pop('lastSync', None)
            doc_dict['launch_date'] = doc_dict.pop('launchDate', None)
            result.append(doc_dict)

        except Exception as e:
            print(f"Error in get_districts: {e}")
        return result

    def get_schools(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('schools').where('districtId', '==', lab_id).get()
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

    def get_classes(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('classes').where('districtId', '==', lab_id).get()
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

    def get_users(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('users').where('districts.current', 'array_contains', lab_id).get()
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

    def get_runs(self, user_id: str):
        result = []
        try:
            docs = self.db.collection('users').document(user_id).collection('runs').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['run_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['task_id'] = doc_dict.pop('taskId', None)
                doc_dict['variant_id'] = doc_dict.pop('variantId', None)
                doc_dict['assignment_id'] = doc_dict.pop('assignmentId', None)
                doc_dict['is_completed'] = doc_dict.pop('completed', None)
                doc_dict['time_finished'] = doc_dict.pop('timeFinished', None)
                doc_dict['time_started'] = doc_dict.pop('timeStarted', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_runs: {e}")
        return result

    def get_trials(self, user_id: str, run_id: str, task_id: str):
        result = []
        try:
            docs = self.db.collection('users').document(user_id).collection('runs').document(run_id).collection(
                'trials').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['run_id'] = run_id
                doc_dict['task_id'] = task_id
                doc_dict['subtask_id'] = doc_dict.pop('subtask', None)
                doc_dict['is_practice'] = True if 'practice' in doc_dict.get('assessment_stage', '') else False
                doc_dict['corpus_id'] = doc_dict.pop('corpusId', None)
                doc_dict['is_correct'] = True if doc_dict.get('correct', '') == 1 else False
                doc_dict['response'] = doc_dict.get('responseValue', None) or doc_dict.get('response',
                                                                                           None) or doc_dict.get(
                    'keyboard_response', None)
                doc_dict['server_timestamp'] = doc_dict.pop('serverTimestamp', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_trails: {e}")
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
                params = doc_dict.get('params', {})
                doc_dict['consent'] = params.get('consent', None)
                doc_dict['recruitment'] = params.get('recruitment', '')
                doc_dict['skip_instructions'] = params.get('skipInstructions', None)
                doc_dict['story'] = params.get('story', None)
                doc_dict['user_mode'] = params.get('user_mode', None)
                doc_dict['last_updated'] = doc_dict.get('lastUpdated', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_variants: {e}")
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


fs_assessment = FirestoreServices(app_name='assessment_site', DB_KEY=settings.DB_KEY_LOCATION_ASSESSMENT)
fs_admin = FirestoreServices(app_name='admin_site', DB_KEY=settings.DB_KEY_LOCATION_ADMIN)


class EntityController:
    def __init__(self, lab_id):
        self.lab_id = lab_id

        self.valid_districts = []
        self.invalid_districts = []
        self.valid_schools = []
        self.invalid_schools = []
        self.valid_classes = []
        self.invalid_classes = []
        self.set_districts()
        self.set_schools()
        self.set_classes()

        self.valid_tasks = []
        self.invalid_tasks = []
        self.valid_variants = []
        self.invalid_variants = []
        self.valid_variants_params = []
        self.invalid_variants_params = []
        self.set_task()

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

        self.valid_runs = []
        self.invalid_runs = []
        # self.valid_score_details = []
        # self.invalid_score_details = []
        self.valid_trials = []
        self.invalid_trials = []
        self.set_runs()
        self.set_trials()
        # self.set_trials()

    def set_districts(self):
        districts = fs_admin.get_districts(lab_id=self.lab_id)
        for district in districts:
            try:
                district = District(**district)
                self.valid_districts.append(district)

            except ValidationError as e:
                print(f"Validation error for district {district['id']}: {e}")
                self.invalid_districts.append(f"{district['id']}: {e.errors()}")

    def set_schools(self):
        schools = fs_admin.get_schools(lab_id=self.lab_id)
        for school in schools:
            try:
                school = School(**school)
                self.valid_schools.append(school)

            except ValidationError as e:
                print(f"Validation error for school {school['id']}: {e}")
                self.invalid_schools.append(f"{school['id']}: {e.errors()}")

    def set_classes(self):
        classes = fs_admin.get_classes(lab_id=self.lab_id)
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
                self.invalid_tasks.append(f"{task['id']}: {e.errors()}")

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
        users = fs_admin.get_users(lab_id=self.lab_id)
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
        if self.valid_users:
            for user in self.valid_users:
                runs = fs_assessment.get_runs(user.id)
                for run in runs:
                    try:
                        self.valid_runs.append(Run(**run))
                    except ValidationError as e:
                        print(f"Validation error for user {user.id}, run {run['id']}: {e}")
                        self.invalid_runs.append(f"{user.id},{run['id']}")
        else:
            print("No valid users.")

    def set_score_details(self, run: dict):
        pass
        # run_id = run.get('id', None)
        # scores = run.get('scores', {})
        # computed_scores = scores.get('computed', {})
        # raw_scores = scores.get('raw', {})
        # for sub_task, sub_score in computed_scores.items():
        #     score = None
        #     if isinstance(sub_score, int) or isinstance(sub_score, str):
        #         score = sub_score
        #     elif isinstance(sub_score, dict):
        #         score = sub_score.get('roarScore', None)
        #         attempted_note = ''
        #         correct_note = ''
        #         incorrect_note = ''
        #         theta_estimate = ''
        #         theta_se = ''
        #         test_result = raw_scores.get(sub_task, {}).get('test', {})
        #         for key, value in test_result.items():
        #             if key.contains('Attempt'):
        #                 attempted_note = value
        #             if key.contains('Correct'):
        #                 correct_note = value
        #             if key.contains('Attempt'):
        #                 attempted_note = value
        #             if key.contains('Correct'):
        #                 correct_note = value
        #
        #
        #     try:
        #         ScoreDetails(run_id=run_id,
        #                      is_computed=True,
        #                      is_composite=True if sub_task == 'composite' else False,
        #                      is_practice=False,
        #                      subtask_name=None if sub_task == 'composite' else sub_task,
        #                      score=score)
        #     except ValidationError as e:
        #         print(f"score_detail error for run {run_id}, computed_scores {key}: {e}")
        #         self.invalid_score_details.append(f"{run_id},{key}")

    def set_trials(self):
        if self.valid_runs:
            for run in self.valid_runs:
                trials = fs_assessment.get_trials(user_id=run.user_id, run_id=run.id, task_id=run.task_id)
                for trial in trials:
                    try:
                        self.valid_trials.append(Trial(**trial))
                    except ValidationError as e:
                        print(f"Validation error for user {run.user_id}, run {run.id}, trial {trial['id']}: {e}")
                        self.invalid_trials.append(f"{run.user_id}, {run.id}, {trial['id']}")
        else:
            print("No valid runs.")
