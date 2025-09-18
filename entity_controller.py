from datetime import datetime, timezone
from pydantic import ValidationError
import logging
import core_models
import settings
from firestore_services import firestore_services as fs, stringify_variables
import utils
import json

logging.basicConfig(level=logging.INFO)

now_utc = datetime.now(timezone.utc).isoformat()


class EntityController:

    def __init__(self, org: utils.Organization):
        self.org = org

        self.validation_log = {"org_info": str(org)}
        self.run_key_usage = {}
        self.trial_key_usage = {}
        self.survey_key_usage = {}
        self.new_schemas = {"runs": [], "trials": [], "surveys": []}

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
        self.valid_user_classes = []
        self.invalid_user_classes = []

        # self.valid_administration_tasks = []
        # self.invalid_administration_tasks = []
        # self.valid_user_assignments = []
        # self.invalid_user_assignments = []

    def adding_schema_row_to_data(self):
        if self.valid_districts:
            self.valid_districts.append(
                core_models.DistrictBase(district_id='schema_row', name='schema_row', abbreviation='',
                                         created_at=now_utc, updated_at=now_utc))
        if self.valid_schools:
            self.valid_schools.append(core_models.SchoolBase(school_id='schema_row', district_id='schema_row',
                                                             name='schema_row', abbreviation='',
                                                             created_at=now_utc, updated_at=now_utc))
        if self.valid_classes:
            self.valid_classes.append(core_models.ClassBase(class_id='schema_row', school_id='schema_row', grade='schema_row',
                                                            district_id='schema_row', name='schema_row', abbreviation='',
                                                            created_at=now_utc, updated_at=now_utc))
        if self.valid_groups:
            self.valid_groups.append(core_models.GroupBase(group_id='schema_row', name='schema_row', abbreviation='',
                                                           tags='', created_at=now_utc, updated_at=now_utc))
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
                                        validation_msg_user='schema_row'))
        if self.valid_runs:
            self.valid_runs.append(
                core_models.LevanteRun(run_id='schema_row', user_id='schema_row', task_id='schema_row',
                                       variant_id='schema_row', administration_id='schema_row',
                                       reliable=False, completed=False, best_run=False,
                                       task_version='', time_started=now_utc,
                                       time_finished=now_utc,
                                       num_attempted=0, num_correct=0, test_comp_theta_estimate=0.0001,
                                       test_comp_theta_se=0.0001, valid_run=False,
                                       warning_msg_run='schema_row', validation_msg_run='schema_row'))
        if self.valid_trials:
            self.valid_trials.append(
                core_models.LevanteTrial(trial_id='schema_row', run_id='schema_row', user_id='schema_row',
                                         task_id='schema_row', assessment_stage='schema_row',
                                         trial_index=0, item='', item_id='', item_uid='',
                                         answer='', response='', correct=False,
                                         difficulty=0.0001, response_source='', time_elapsed=0,
                                         rt='', server_timestamp=now_utc, is_practice_trial=False,
                                         corpus_trial_type='', corpus_id='schema_row',
                                         response_type='', response_location='',
                                         distractors='', theta_estimate=0.0001,
                                         theta_estimate2=0.0001,
                                         theta_se=0.0001, theta_se2=0.0001, valid_trial=False,
                                         warning_msg_trial='schema_row',
                                         validation_msg_trial='schema_row'))

    def validate_data_from_firestore(self):
        # Determine whether it's using guest.
        if not self.org.is_guest:
            if self.org.filters.org_filter.key == 'districts':
                self.process_districts()
                self.process_schools()
                self.process_classes()
                self.process_groups()
            elif self.org.filters.org_filter.key == 'groups':
                self.process_groups()

            self.process_administration()

        self.process_users()

        if self.valid_users:
            if not self.org.is_guest:
                self.process_surveys()
            self.process_runs()
            if self.valid_runs:
                self.process_trials()
                self.process_tasks_variants()

        self.adding_schema_row_to_data()

        # Track schema
        for task in self.valid_tasks:
            if task.task_id == 'survey':
                for survey_type, schema_dict in self.survey_key_usage.items():
                    fs.upload_task_schema_to_firestore(dict_type=survey_type, schema_usage=self.survey_key_usage,
                                                       task_id=task.task_id, new_schemas=self.new_schemas['surveys'])
            else:
                fs.upload_task_schema_to_firestore(dict_type='runKeys', schema_usage=self.run_key_usage,
                                                   task_id=task.task_id, new_schemas=self.new_schemas['runs'])
                fs.upload_task_schema_to_firestore(dict_type='trialKeys', schema_usage=self.trial_key_usage,
                                                   task_id=task.task_id, new_schemas=self.new_schemas['trials'])

        # with open('new_schemas.json', 'w', encoding='utf-8') as f:
        #     json.dump(self.new_schemas, f, cls=utils.CustomJSONEncoder)

    def get_validated_data(self):
        data = {
            'districts': [obj.model_dump() for obj in self.valid_districts],
            'schools': [obj.model_dump() for obj in self.valid_schools],
            'classes': [obj.model_dump() for obj in self.valid_classes],
            'groups': [obj.model_dump() for obj in self.valid_groups],
            'administrations': [obj.model_dump() for obj in self.valid_administrations],
            'tasks': [obj.model_dump() for obj in self.valid_tasks],
            'variants': [obj.model_dump() for obj in self.valid_variants],
            'users': [obj.model_dump() for obj in self.valid_users],
            'runs': [obj.model_dump() for obj in self.valid_runs],
            'trials': [obj.model_dump() for obj in self.valid_trials],
            'survey_responses': [obj.model_dump() for obj in self.valid_survey_responses],
            'user_groups': [obj.model_dump() for obj in self.valid_user_groups],
            'user_classes': [obj.model_dump() for obj in self.valid_user_classes],
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

    def process_districts(self):
        logging.info("Now Validating Districts...")

        districts = fs.get_districts_by_district_name_list(date_filter=self.org.filters.date_filter,
                                                           district_name_list=self.org.filters.org_filter.value)

        self.set_districts(districts=districts)

    def process_schools(self):
        logging.info("Now Validating Schools...")

        schools = fs.get_schools_by_district_ids(district_ids=[d.district_id for d in self.valid_districts])

        self.set_schools(schools=schools)

    def process_classes(self):
        logging.info("Now Validating Classes...")

        classes = fs.get_classes_by_school_ids(school_ids=[s.school_id for s in self.valid_schools])

        self.set_classes(classes=classes)

    def process_groups(self):
        logging.info("Now Validating Groups...")

        if self.org.filters.org_filter.key == 'districts':
            groups = fs.get_groups_by_district_ids(district_ids=[d.district_id for d in self.valid_districts])
        elif self.org.filters.org_filter.key == 'groups':
            groups = fs.get_groups_by_group_names(date_filter=self.org.filters.date_filter,
                                                  group_names_list=self.org.filters.org_filter.value)
        else:
            groups = []

        self.set_groups(groups=groups)

    def process_administration(self):
        logging.info("Now Validating Administrations...")

        group_ids = [group.group_id for group in self.valid_groups]
        district_ids = [district.district_id for district in self.valid_districts]
        school_ids = [schools.school_id for schools in self.valid_schools]
        administrations = fs.get_administrations(date_filter=self.org.filters.date_filter,
                                                 group_ids=group_ids,
                                                 district_ids=district_ids,
                                                 school_ids=school_ids)
        self.set_administrations(administrations=administrations)
        # input(f"districts: {[f"{d.name}+{d.district_id}" for d in self.valid_districts]}")
        # input(f"groups: {[f"{g.name}+{g.group_id}" for g in self.valid_groups]}")
        # input(f"admins: {[f"{a.name}+{a.administration_id}" for a in self.valid_administrations]}")

    def process_tasks_variants(self):
        logging.info("Now Validating Tasks and Variants...")
        task_variants = {}
        for item in self.valid_runs:
            if item.task_id not in task_variants:
                task_variants[item.task_id] = []
            if item.variant_id and item.variant_id not in task_variants[item.task_id]:
                task_variants[item.task_id].append(item.variant_id)

        tasks = fs.get_tasks(task_filter=list(task_variants.keys()))
        self.set_tasks(tasks=tasks)

        if self.valid_tasks:
            for task in self.valid_tasks:
                variants = fs.get_variants(task_id=task.task_id,
                                           variant_filter=task_variants.get(task.task_id, []))
                self.set_variants(variants=variants, task_id=task.task_id)

    def process_users(self):
        logging.info("Now Validating Users and UserGroups...")

        if self.org.filters.org_filter.key == 'groups':
            org_ids = [group.group_id for group in self.valid_groups]
        elif self.org.filters.org_filter.key == 'districts':
            org_ids = [district.district_id for district in self.valid_districts]
        else:
            org_ids = []

        users = fs.get_users(is_guest=self.org.is_guest,
                             date_filter=self.org.filters.date_filter,
                             org_filter=self.org.filters.org_filter,
                             org_ids=org_ids,
                             user_filter=self.org.filters.user_filter)

        self.set_users(users=users)

    def process_surveys(self):
        logging.info("Now Validating Surveys...")
        for user in self.valid_users:
            survey_responses = fs.get_surveys(user_id=user.user_id,
                                              user_type=user.user_type,
                                              date_filter=self.org.filters.date_filter,
                                              survey_key_usage=self.survey_key_usage)
            if survey_responses:
                self.set_survey_responses(user=user, survey_responses=survey_responses)
                if user.user_type == 'student':
                    self.survey_responses_stats["student"] += 1
                elif user.user_type == 'teacher':
                    self.survey_responses_stats["teacher"] += 1
                elif user.user_type == 'parent':
                    self.survey_responses_stats["caregiver"] += 1

    def process_runs(self):
        logging.info("Now Validating Runs...")
        for user in self.valid_users:
            self.set_runs(user_id=user.user_id, runs=fs.get_runs(user_id=user.user_id,
                                                                 run_key_usage=self.run_key_usage,
                                                                 is_guest=self.org.is_guest))

    def process_trials(self):
        logging.info("Now Validating Trials...")
        for run in self.valid_runs:
            self.set_trials(run=run,
                            trials=fs.get_trials(user_id=run.user_id,
                                                 run_id=run.run_id,
                                                 task_id=run.task_id,
                                                 is_guest=self.org.is_guest,
                                                 trial_key_usage=self.trial_key_usage))
            run.validate_trials_in_run()

    def set_groups(self, groups: list):
        for group in groups:
            try:
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
                task = core_models.TaskBase(**task)
                self.valid_tasks.append(task)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_tasks.append({**error, 'id': task['task_id']})

    def set_variants(self, variants: list, task_id: str):
        for variant in variants:
            try:
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
                else:
                    user = core_models.UserBase(**user)
                self.valid_users.append(user)
                self.set_user_group_class(user=user_dict)

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

    def set_user_group_class(self, user: dict):
        user_id = user.get('user_id', None)
        user_groups = user.get('groups', {})
        all_groups = user_groups.get('all', [])
        current_groups = user_groups.get('current', [])

        user_classes = user.get('classes', {})
        all_classes = user_classes.get('all', [])
        current_classes = user_classes.get('current', [])

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

        def append_to_classes(class_id, is_active):
            try:
                self.valid_user_classes.append(core_models.UserClass(
                    user_id=user_id,
                    class_id=class_id,
                    is_active=is_active))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_classes.append(
                        {**error, 'id': f"user_id: {user_id}, class_id: {class_id}"})

        if not self.org.is_guest:
            for group in all_groups:
                append_to_groups(group, group in current_groups)  # Set is_active based on presence in current_groups
            for classes in all_classes:
                append_to_classes(classes, classes in current_classes)

    def set_administrations(self, administrations: list):
        for administration in administrations:
            try:
                administration = core_models.AdministrationBase(**administration)
                self.valid_administrations.append(administration)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_administrations.append({**error, 'id': administration['assignment_id']})

    def set_runs(self, user_id: str, runs: list):
        for run in runs:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    self.valid_runs.append(core_models.LevanteRun(**run))
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
                else:
                    trial_model = core_models.TrialBase(**trial)
                self.valid_trials.append(trial_model)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_trials.append(
                        {**error,
                         'id': f"trial_id: {trial['trial_id']}, run_id: {run.run_id}, user_id: {run.user_id}, task_id:{trial.get('task_id', None)}"})

    # def set_user_assignment(self, user: dict):
    #     user_id = user.get('user_id', None)
    #     assignments_assigned = user.get('assignmentsAssigned', {})
    #     assignments_started = user.get('assignmentsStarted', {})
    #     for key, value in assignments_assigned.items():
    #         try:
    #             self.valid_user_assignments.append(core_models.UserAssignment(
    #                 user_id=user_id,
    #                 assignment_id=key,
    #                 started=True if key in assignments_started.keys() else False,
    #                 date_time=value))
    #         except ValidationError as e:
    #             for error in e.errors():
    #                 self.invalid_user_assignments.append(
    #                     {**error, 'id': f"user_id: {user['user_id']}, assignment: {key}"})

    # def set_administration_task(self, administration: dict):
    #     administration_id = administration.get('administration_id', None)
    #     tasks = administration.get('assessments', [])
    #     for task in tasks:
    #         task_id = task.get('taskId', None)
    #         variant_id = task.get('variantId', None)
    #         try:
    #             self.valid_administration_tasks.append(
    #                 core_models.AdministrationTask(administration_id=administration_id,
    #                                                task_id=task_id,
    #                                                variant_id=variant_id))
    #         except ValidationError as e:
    #             for error in e.errors():
    #                 self.invalid_administration_tasks.append(
    #                     {**error,
    #                      'id': f"administration_id:{administration['administration_id']}, task_id:{task_id}, variant_id:{variant_id}"})
