from datetime import datetime, timezone
from pydantic import ValidationError
import logging
import core_models
import settings
from firestore_services import FirestoreServices, stringify_variables
from utils import Organization

logging.basicConfig(level=logging.INFO)

now_utc = datetime.now(timezone.utc).isoformat()


class EntityController:

    def __init__(self, org: Organization):
        self.org = org

        self.validation_log = {"org_info": str(org)}
        self.valid_groups = []
        self.invalid_groups = []
        self.valid_administrations = []
        self.invalid_administrations = []
        self.valid_districts = []
        self.invalid_districts = []
        self.valid_schools = []
        self.invalid_schools = []
        self.valid_classes = []
        self.invalid_classes = []
        self.valid_tasks = []
        self.invalid_tasks = []
        self.valid_variants = []
        self.invalid_variants = []

        self.valid_users = []
        self.invalid_users = []
        self.valid_runs = []

        self.invalid_runs = []
        self.valid_trials = []
        self.invalid_trials = []

        self.valid_survey_responses = []
        self.invalid_survey_responses = []
        self.survey_responses_stats = {"student": 0, "teacher": 0, "caregiver": 0}

        self.valid_user_groups = []
        self.invalid_user_groups = []
        self.valid_administration_tasks = []
        self.invalid_administration_tasks = []

        self.valid_user_assignments = []
        self.invalid_user_assignments = []

    def adding_schema_row_to_data(self):
        if self.valid_groups:
            self.valid_groups.append(core_models.LevanteGroup(group_id='schema_row', name='schema_row', abbreviation='',
                                                              tags='', created_at=now_utc))
        if self.valid_tasks:
            self.valid_tasks.append(core_models.TaskBase(task_id='schema_row', name='schema_row', description='',
                                                         last_updated=now_utc))
        if self.valid_administrations:
            self.valid_administrations.append(
                core_models.AdministrationBase(administration_id='schema_row', name='schema_row', public_name='',
                                               sequential=False,
                                               created_by='schema_row', date_created=now_utc, date_closed=now_utc,
                                               date_opened=now_utc))
        if self.valid_variants:
            self.valid_variants.append(
                core_models.VariantBase(variant_id='schema_row', task_id='schema_row', name='', age=0,
                                        button_layout='',
                                        corpus='', key_helpers=False, language='', max_incorrect=0,
                                        max_time=0, num_of_practice_trials=0,
                                        number_of_trials=0, sequential_practice=False, sequential_stimulus=False,
                                        skip_instructions=False,
                                        stimulus_blocks=0, store_item_id=False, last_updated=now_utc))

        if self.valid_users:
            self.valid_users.append(
                core_models.LevanteUser(user_id='schema_row', user_type='schema_row', assessment_pid='schema_row',
                                        assessment_uid='schema_row', email='',
                                        email_verified=False, created_at=now_utc, last_updated=now_utc,
                                        parent1_id='', parent2_id='',
                                        teacher_id='', birth_year=0, birth_month=0, sex='', grade=0,
                                        validation_msg_user=['schema_row']))
        if self.valid_runs:
            self.valid_runs.append(
                core_models.LevanteRun(run_id='schema_row', user_id='schema_row', task_id='schema_row',
                                       variant_id='schema_row', administration_id='schema_row',
                                       reliable=False, completed=False, best_run=False,
                                       task_version='', time_started=now_utc,
                                       time_finished=now_utc,
                                       num_attempted=0, num_correct=0, test_comp_theta_estimate=0.0001,
                                       test_comp_theta_se=0.0001, valid_run=False, validation_msg_run=['schema_row']))
        if self.valid_trials:
            self.valid_trials.append(
                core_models.LevanteTrial(trial_id='schema_row', run_id='schema_row', user_id='schema_row',
                                         task_id='schema_row', assessment_stage='schema_row',
                                         trial_index=0, item='', item_id='',
                                         answer='', response='', correct=False,
                                         difficulty=0.0001, response_source='', time_elapsed=0,
                                         rt='', server_timestamp=now_utc, is_practice_trial=False,
                                         corpus_trial_type='',
                                         response_type='', response_location='',
                                         distractors='', theta_estimate=0.0001,
                                         theta_estimate2=0.0001,
                                         theta_se=0.0001, theta_se2=0.0001, valid_trial=False,
                                         validation_msg_trial=['schema_row']))

    def validate_data_from_firestore(self):
        fs_assessment = FirestoreServices(app_name='assessment_site',
                                          start_date=self.org.filters.date_filter.start_date,
                                          end_date=self.org.filters.date_filter.end_date)
        # Determine whether it's using guest.
        if not self.org.is_guest:
            fs_admin = FirestoreServices(app_name='admin_site',
                                         start_date=self.org.filters.date_filter.start_date,
                                         end_date=self.org.filters.date_filter.end_date)

            if self.org.filters.org_filter.key == 'districts':
                self.process_districts(fs_admin)
                self.process_schools(fs_admin)
                self.process_classes(fs_admin)
            elif self.org.filters.org_filter.key == 'groups' or not self.org.filters.org_filter:
                self.process_groups(fs_admin)

            self.process_administration(fs_admin)

            self.process_tasks_variants(fs_assessment)

            self.process_users(fs=fs_admin)

            if self.valid_users:
                self.process_surveys(fs_admin)
        else:
            self.process_users(fs=fs_assessment)

        if self.valid_users:
            self.process_runs(fs=fs_assessment)
            if self.valid_runs:
                self.process_trials(fs=fs_assessment)

        self.adding_schema_row_to_data()

    def get_validated_data(self):
        data = {
            'groups': [obj.model_dump() for obj in self.valid_groups],
            'administrations': [obj.model_dump() for obj in self.valid_administrations],
            'tasks': [obj.model_dump() for obj in self.valid_tasks],
            'variants': [obj.model_dump() for obj in self.valid_variants],
            'users': [obj.model_dump() for obj in self.valid_users],
            'runs': [obj.model_dump() for obj in self.valid_runs],
            'trials': [obj.model_dump() for obj in self.valid_trials],
            'survey_responses': [obj.model_dump() for obj in self.valid_survey_responses],
            'user_groups': [obj.model_dump() for obj in self.valid_user_groups],
            'administration_tasks': [obj.model_dump() for obj in self.valid_administration_tasks],
        }
        invalid_data = self.get_invalid_data()
        if invalid_data:
            data['invalid_data'] = invalid_data
        return data

    def get_invalid_data(self):
        invalid_list = ([{**obj, "table_name": "groups"} for obj in self.invalid_groups]
                        + [{**obj, "table_name": "districts"} for obj in self.invalid_districts]
                        + [{**obj, "table_name": "schools"} for obj in self.invalid_schools]
                        + [{**obj, "table_name": "classes"} for obj in self.invalid_classes]
                        + [{**obj, "table_name": "tasks"} for obj in self.invalid_tasks]
                        + [{**obj, "table_name": "variants"} for obj in self.invalid_variants]
                        + [{**obj, "table_name": "assignments"} for obj in self.invalid_administrations]
                        + [{**obj, "table_name": "assignment_tasks"} for obj in self.invalid_administrations]
                        + [{**obj, "table_name": "users"} for obj in self.invalid_users]
                        + [{**obj, "table_name": "user_group"} for obj in self.invalid_user_groups]
                        + [{**obj, "table_name": "survey_responses"} for obj in self.invalid_survey_responses]
                        + [{**obj, "table_name": "runs"} for obj in self.invalid_runs]
                        + [{**obj, "table_name": "trials"} for obj in self.invalid_trials]
                        )

        for invalid_item in invalid_list:
            if 'loc' in invalid_item:
                invalid_item['invalid_key'] = stringify_variables(invalid_item.pop('loc')[0]) if len(
                    invalid_item['loc']) > 0 else stringify_variables(invalid_item.pop('loc'))
            if 'input' in invalid_item:
                invalid_item['invalid_value'] = stringify_variables(invalid_item.pop('input'))
            if 'type' in invalid_item:
                invalid_item['expected_value'] = stringify_variables(invalid_item.pop('type'))
            if 'url' in invalid_item:
                invalid_item.pop('url')
            if 'ctx' in invalid_item:
                invalid_item.pop('ctx')

        return invalid_list

    def process_groups(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Groups...")

        groups = fs_admin.get_groups(group_filter=self.org.filters.org_filter.value)

        self.set_groups(groups=groups)

    def process_districts(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Districts...")

        districts = fs_admin.get_districts_by_district_name_list(district_name_list=self.org.filters.org_filter.value)

        self.set_districts(districts=districts)

    def process_schools(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Schools...")

        schools = fs_admin.get_schools(district_id=self.valid_districts[0].district_id)

        self.set_schools(schools=schools)

    def process_classes(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Classes...")

        classes = fs_admin.get_classes(district_id=self.valid_districts[0].district_id)

        self.set_classes(classes=classes)

    def process_administration(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Administration and AdministrationTasks...")

        if self.org.filters.org_filter.key == 'groups':
            org_ids = [group.group_id for group in self.valid_groups]
        elif self.org.filters.org_filter.key == 'districts':
            org_ids = [district.district_id for district in self.valid_districts]
        else:
            org_ids = []
        administrations = fs_admin.get_administrations(org_key=self.org.filters.org_filter.key,
                                                       org_operator=self.org.filters.org_filter.operator,
                                                       org_value=org_ids)

        self.set_administrations(administrations=administrations)

    def process_tasks_variants(self, fs_assessment: FirestoreServices):
        logging.info("Now Validating Tasks and Variants...")
        task_variants = {}
        if self.valid_administration_tasks:
            for item in self.valid_administration_tasks:
                if item.task_id not in task_variants:
                    task_variants[item.task_id] = []
                if item.variant_id and item.variant_id not in task_variants[
                    item.task_id]:  # Only add non-None non-exists variant_id
                    task_variants[item.task_id].append(item.variant_id)
        tasks = fs_assessment.get_tasks(task_filter=list(task_variants.keys()))
        self.set_tasks(tasks=tasks)

        if self.valid_tasks:
            for task in self.valid_tasks:
                variants = fs_assessment.get_variants(task_id=task.task_id,
                                                      variant_filter=task_variants.get(task.task_id, []))
                self.set_variants(variants=variants, task_id=task.task_id)

    def process_users(self, fs: FirestoreServices):
        logging.info("Now Validating Users and UserGroups...")

        if self.org.filters.org_filter.key == 'groups':
            org_ids = [group.group_id for group in self.valid_groups]
        elif self.org.filters.org_filter.key == 'districts':
            org_ids = [district.district_id for district in self.valid_districts]
        else:
            org_ids = []

        users = fs.get_users(is_guest=self.org.is_guest,
                             org_key=self.org.filters.org_filter.key, org_operator=self.org.filters.org_filter.operator,
                             org_value=org_ids,
                             user_key=self.org.filters.user_filter.key,
                             user_operator=self.org.filters.user_filter.operator,
                             user_value=self.org.filters.user_filter.value,
                             is_using_full_users_list=False)

        self.set_users(users=users)

    def process_surveys(self, fs_admin: FirestoreServices):
        logging.info("Now Validating Surveys...")
        for user in self.valid_users:
            survey_responses = fs_admin.get_surveys(user_id=user.user_id,
                                                    user_type=user.user_type)
            if survey_responses:
                self.set_survey_responses(user=user, survey_responses=survey_responses)
                if user.user_type == 'student':
                    self.survey_responses_stats["student"] += 1
                elif user.user_type == 'teacher':
                    self.survey_responses_stats["teacher"] += 1
                elif user.user_type == 'parent':
                    self.survey_responses_stats["caregiver"] += 1

    def process_runs(self, fs: FirestoreServices):
        logging.info("Now Validating Runs...")
        for user in self.valid_users:
            self.set_runs(user_id=user.user_id, runs=fs.get_runs(user_id=user.user_id,
                                                                 is_guest=self.org.is_guest))

    def process_trials(self, fs: FirestoreServices):
        logging.info("Now Validating Trials...")
        for run in self.valid_runs:
            self.set_trials(run=run,
                            trials=fs.get_trials(user_id=run.user_id,
                                                 run_id=run.run_id,
                                                 task_id=run.task_id,
                                                 is_guest=self.org.is_guest))
            run.validate_trials_in_run()

    def set_groups(self, groups: list):
        for group in groups:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    group = core_models.LevanteGroup(**group)
                elif settings.config['INSTANCE'] == 'ROAR':
                    group = core_models.RoarGroup(**group)
                else:
                    group = core_models.GroupBase(**group)
                self.valid_groups.append(group)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_groups.append({**error, 'id': group["group_id"]})

    def set_districts(self, districts: list):
        for district in districts:
            try:
                district = core_models.DistrictBase(**district)
                self.valid_districts.append(district)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_districts.append({**error, 'id': district["district_id"]})

    def set_schools(self, schools: list):
        for school in schools:
            try:
                school = core_models.SchoolBase(**school)
                self.valid_schools.append(school)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_schools.append({**error, 'id': school['school_id']})

    def set_classes(self, classes: list):
        for c in classes:
            try:
                c = core_models.SchoolBase(**c)
                self.valid_classes.append(c)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_classes.append({**error, 'id': c['class_id']})

    def set_tasks(self, tasks: list):
        for task in tasks:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    task = core_models.TaskBase(**task)
                elif settings.config['INSTANCE'] == 'ROAR':
                    task = core_models.RoarTask(**task)
                else:
                    task = core_models.TaskBase(**task)
                self.valid_tasks.append(task)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_tasks.append({**error, 'id': task['task_id']})

    def set_variants(self, variants: list, task_id: str):
        for variant in variants:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    variant = core_models.VariantBase(**variant)
                elif settings.config['INSTANCE'] == 'ROAR':
                    variant = core_models.VariantBase(**variant)
                else:
                    variant = core_models.VariantBase(**variant)

                self.valid_variants.append(variant)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_variants.append(
                        {**error, 'id': f"variant_id: {variant['variant_id']}, task_id: {task_id}"})

    def set_users(self, users):
        for user in users:
            user_dict = user
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    core_models.LevanteUser.set_valid_groups(self.valid_groups)
                    user = core_models.LevanteUser(**user)
                elif settings.config['INSTANCE'] == 'ROAR':
                    user = core_models.UserBase(**user)
                else:
                    user = core_models.UserBase(**user)
                self.valid_users.append(user)
                self.set_user_group(user=user_dict)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_users.append({**error, 'id': user_dict['user_id']})

    def set_survey_responses(self, user: core_models.LevanteUser, survey_responses: list):
        for survey_response in survey_responses:
            try:
                self.valid_survey_responses.append(core_models.SurveyResponse(**survey_response))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_survey_responses.append(
                        {**error, 'id': f"user_id: {user.user_id}, survey_id: {survey_response['survey_response_id']}"})

    def set_user_group(self, user: dict):
        user_id = user.get('user_id', None)
        user_groups = user.get('groups', {})
        all_groups = user_groups.get('all', [])
        current_groups = user_groups.get('current', [])

        def append_to_groups(group_id, is_active):
            try:
                self.valid_user_groups.append(core_models.UserGroup(
                    user_id=user_id,
                    group_id=group_id,
                    is_active=is_active))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_groups.append(
                        {**error, 'id': f"user_id: {user_id}, group_id: {group_id}"})

        if self.org.is_guest:
            append_to_groups(self.org.org_id, True)  # Guest users are always active in their org group
        else:
            for group in all_groups:
                append_to_groups(group,
                                 group in current_groups)  # Set is_active based on presence in current_groups

    def set_administrations(self, administrations: list):
        for administration in administrations:
            administration_dict = administration
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    administration = core_models.AdministrationBase(**administration)
                elif settings.config['INSTANCE'] == 'ROAR':
                    administration = core_models.RoarAdministration(**administration)
                else:
                    administration = core_models.AdministrationBase(**administration)
                self.valid_administrations.append(administration)
                self.set_administration_task(administration_dict)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_administrations.append({**error, 'id': administration['assignment_id']})

    def set_administration_task(self, administration: dict):
        administration_id = administration.get('administration_id', None)
        tasks = administration.get('assessments', [])
        for task in tasks:
            task_id = task.get('taskId', None)
            variant_id = task.get('variantId', None)
            try:
                self.valid_administration_tasks.append(
                    core_models.AdministrationTask(administration_id=administration_id,
                                                   task_id=task_id,
                                                   variant_id=variant_id))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_administration_tasks.append(
                        {**error,
                         'id': f"administration_id:{administration['administration_id']}, task_id:{task_id}, variant_id:{variant_id}"})

    def set_runs(self, user_id: str, runs: list):
        for run in runs:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    self.valid_runs.append(core_models.LevanteRun(**run))
                elif settings.config['INSTANCE'] == 'ROAR':
                    self.valid_runs.append(core_models.RoarRun(**run))
                else:
                    self.valid_trials.append(core_models.RunBase(**run))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_runs.append(
                        {**error, 'id': f"run_id: {run['run_id']}, user_id: {user_id}"})

    def set_trials(self, run: core_models.RunBase, trials: list):
        for trial in trials:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    trial_model = core_models.LevanteTrial(**trial)
                    is_test_trial = (trial_model.assessment_stage == 'test_response' or
                                     (trial_model.is_practice_trial is not None and not trial_model.is_practice_trial))
                    # Execute action based on the condition
                    if is_test_trial:
                        run.add_non_practice_trials(trial_model)
                elif settings.config['INSTANCE'] == 'ROAR':
                    trial_model = core_models.RoarTrial(**trial)
                else:
                    trial_model = core_models.TrialBase(**trial)
                self.valid_trials.append(trial_model)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_trials.append(
                        {**error,
                         'id': f"trial_id: {trial['trial_id']}, run_id: {run.run_id}, user_id: {run.user_id}, task_id:{trial.get('task_id', None)}"})

    def set_user_assignment(self, user: dict):
        user_id = user.get('user_id', None)
        assignments_assigned = user.get('assignmentsAssigned', {})
        assignments_started = user.get('assignmentsStarted', {})
        for key, value in assignments_assigned.items():
            try:
                self.valid_user_assignments.append(core_models.UserAssignment(
                    user_id=user_id,
                    assignment_id=key,
                    started=True if key in assignments_started.keys() else False,
                    date_time=value))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_assignments.append(
                        {**error, 'id': f"user_id: {user['user_id']}, assignment: {key}"})
