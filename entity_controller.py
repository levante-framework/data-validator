from pydantic import ValidationError
import os
import logging
import core_models
import settings
from firestore_services import FirestoreServices, stringify_variables

logging.basicConfig(level=logging.INFO)


class EntityController:

    def __init__(self):
        self.validation_log = {}

        self.valid_groups = []
        self.invalid_groups = []
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
        self.valid_user_groups = []
        self.invalid_user_groups = []
        self.valid_user_classes = []
        self.invalid_user_classes = []
        self.valid_user_assignments = []
        self.invalid_user_assignments = []

        self.valid_assignments = []
        self.invalid_assignments = []
        self.valid_assignment_tasks = []
        self.invalid_assignment_tasks = []

        self.valid_runs = []
        self.invalid_runs = []
        self.valid_trials = []
        self.invalid_trials = []

        self.valid_survey_responses = []
        self.invalid_survey_responses = []

        # self.valid_score_details = []
        # self.invalid_score_details = []
        # self.valid_variants_params = []
        # self.invalid_variants_params = []

    def set_values_from_firestore(self, lab_id: str, start_date, end_date, filter_by, filter_list):
        fs_assessment = FirestoreServices(app_name='assessment_site', start_date=start_date, end_date=end_date)
        fs_admin = FirestoreServices(app_name='admin_site', start_date=start_date, end_date=end_date)

        if os.environ.get('guest_mode', None):
            logging.info("GUEST MODE:")
        else:
            logging.info("REGISTERED USER MODE:")

        logging.info("Now Validating Groups...")
        if settings.config['INSTANCE'] == 'ROAR':
            self.set_groups(groups=fs_admin.get_groups(lab_id=lab_id))
        elif settings.config['INSTANCE'] == 'LEVANTE':
            self.set_groups(groups=fs_admin.get_groups_by_group_name_list(group_name_list=filter_list if filter_by == 'groups' else None))
        else:
            logging.info("Can't set groups without specifying instance.")

        groups_result = {"Valid": len(self.valid_groups),
                         "Invalid": len(self.invalid_groups),
                         }
        logging.info(f"groups: {groups_result}")
        self.validation_log['groups'] = groups_result

        if settings.config['INSTANCE'] == 'ROAR':
            logging.info("Now Validating Districts...")
            self.set_districts(districts=fs_admin.get_districts(lab_id=lab_id))
            districts_result = {"Valid": len(self.valid_districts),
                                "Invalid": len(self.invalid_districts),
                                }
            logging.info(districts_result)
            self.validation_log['districts'] = districts_result

            logging.info("Now Validating Schools...")
            self.set_schools(schools=fs_admin.get_schools(lab_id=lab_id))
            schools_result = {"Valid": len(self.valid_schools),
                              "Invalid": len(self.invalid_schools),
                              }
            logging.info(schools_result)
            self.validation_log['schools'] = schools_result

            logging.info("Now Validating Classes...")
            self.set_classes(classes=fs_admin.get_classes(lab_id=lab_id))
            classes_result = {"Valid": len(self.valid_classes),
                              "Invalid": len(self.invalid_classes),
                              }
            logging.info(classes_result)
            self.validation_log['classes'] = classes_result

        logging.info("Now Validating Tasks and Variants...")
        self.set_tasks(tasks=fs_assessment.get_tasks())
        if self.valid_tasks:
            for task in self.valid_tasks:
                self.set_variants(variants=fs_assessment.get_variants(task.task_id), task_id=task.task_id)
            tasks_result = {"Valid": len(self.valid_tasks),
                            "Invalid": len(self.invalid_tasks),
                            }
            variants_result = {"Valid": len(self.valid_variants),
                               "Invalid": len(self.invalid_variants),
                               }
            self.validation_log['variants'] = variants_result
            logging.info(f"variants: {variants_result}")
        else:
            tasks_result = "No valid tasks were found."
        self.validation_log['tasks'] = tasks_result
        logging.info(f"tasks: {tasks_result}")

        logging.info("Now Validating Assignments and AssignmentTasks...")
        if settings.config['INSTANCE'] == 'ROAR':
            self.set_assignments(assignments=fs_admin.get_assignments(filter_list=[lab_id], filter_by='districts'))
        elif settings.config['INSTANCE'] == 'LEVANTE':
            self.set_assignments(assignments=fs_admin.get_assignments(filter_list=[group.group_id for group in self.valid_groups] if filter_by == 'groups' else None,
                                                                      filter_by=filter_by))
        else:
            logging.info("Can't set assignments without specifying instance.")

        assignments_result = {"Valid": len(self.valid_assignments),
                              "Invalid": len(self.invalid_assignments),
                              }
        logging.info(f"assignments: {assignments_result}")
        self.validation_log['assignments'] = assignments_result

        assignment_task_result = {"Valid": len(self.valid_assignment_tasks),
                                  "Invalid": len(self.invalid_assignment_tasks),
                                  }
        logging.info(f"assignment_tasks: {assignment_task_result}")
        self.validation_log['assignment_tasks'] = assignment_task_result

        logging.info("Now Validating Users and UserGroups...")
        if settings.config['INSTANCE'] == 'ROAR':
            self.set_users(users=fs_admin.get_users(filter_list=[lab_id], filter_by='districts'))
        elif settings.config['INSTANCE'] == 'LEVANTE':
            self.set_users(users=fs_admin.get_users(filter_list=[group.group_id for group in self.valid_groups] if filter_by == 'groups' else None,
                                                    filter_by=filter_by))
        else:
            logging.info("Can't set users without specifying instance.")

        users_result = {"Valid": len(self.valid_users),
                        "Invalid": len(self.invalid_users),
                        }
        logging.info(f"users: {users_result}")
        self.validation_log['users'] = users_result

        user_group_result = {"Valid": len(self.valid_user_groups),
                             "Invalid": len(self.invalid_user_groups),
                             }
        logging.info(f"user_groups: {user_group_result}")
        self.validation_log['user_groups'] = user_group_result

        if self.valid_users:
            logging.info("Now Validating SurveyResponses...")
            for user in self.valid_users:
                self.set_survey_responses(user=user,
                                          survey_responses=fs_admin.get_survey_responses(user_id=user.user_id))
            survey_responses_result = {"Valid": len(self.valid_survey_responses),
                                       "Invalid": len(self.invalid_survey_responses),
                                       }
            logging.info(survey_responses_result)
            self.validation_log['survey_responses'] = survey_responses_result

            logging.info("Now Validating Runs...")
            for user in self.valid_users:
                self.set_runs(user_id=user.user_id, runs=fs_assessment.get_runs(user_id=user.user_id))
            runs_result = {"Valid": len(self.valid_runs),
                           "Invalid": len(self.invalid_runs),
                           }
            logging.info(f"runs: {runs_result}")
            self.validation_log['runs'] = runs_result

            logging.info("Now Validating Trials...")
            if self.valid_runs:
                for run in self.valid_runs:
                    self.set_trials(user_id=run.user_id,
                                    run_id=run.run_id,
                                    trials=fs_assessment.get_trials(user_id=run.user_id,
                                                                    run_id=run.run_id,
                                                                    task_id=run.task_id))
                trials_result = {"Valid": len(self.valid_trials),
                                 "Invalid": len(self.invalid_trials),
                                 }
                logging.info(f"trials: {trials_result}")
                self.validation_log['trials'] = trials_result
            else:
                self.validation_log['runs'] = "No valid runs were found."
                self.validation_log['trials'] = "No valid trials were found."
                logging.info("Runs result: No valid runs were found.")
        else:
            self.validation_log['survey_responses'] = "No valid survey_responses were found."
            self.validation_log['users'] = "No valid users were found."
            self.validation_log['runs'] = "No valid runs were found."
            self.validation_log['trials'] = "No valid trials were found."
            logging.info("Users result: No valid users were found.")

    def get_valid_data(self):
        valid_dict = {
            # 'districts': [obj.model_dump() for obj in self.valid_districts],
            # 'schools': [obj.model_dump() for obj in self.valid_schools],
            # 'classes': [obj.model_dump() for obj in self.valid_classes],
            'groups': [obj.model_dump() for obj in self.valid_groups],
            'tasks': [obj.model_dump() for obj in self.valid_tasks],
            'variants': [obj.model_dump() for obj in self.valid_variants],
            'assignments': [obj.model_dump() for obj in self.valid_assignments],
            'assignment_tasks': [obj.model_dump() for obj in self.valid_assignment_tasks],
            'users': [obj.model_dump() for obj in self.valid_users],
            'user_groups': [obj.model_dump() for obj in self.valid_user_groups],
            'survey_responses': [obj.model_dump() for obj in self.valid_survey_responses],
            'runs': [obj.model_dump() for obj in self.valid_runs],
            'trials': [obj.model_dump() for obj in self.valid_trials],
        }
        return valid_dict

    def get_invalid_data(self):
        invalid_list = ([{**obj, "table_name": "groups"} for obj in self.invalid_groups]
                        + [{**obj, "table_name": "districts"} for obj in self.invalid_districts]
                        + [{**obj, "table_name": "schools"} for obj in self.invalid_schools]
                        + [{**obj, "table_name": "classes"} for obj in self.invalid_classes]
                        + [{**obj, "table_name": "tasks"} for obj in self.invalid_tasks]
                        + [{**obj, "table_name": "variants"} for obj in self.invalid_variants]
                        + [{**obj, "table_name": "assignments"} for obj in self.invalid_assignments]
                        + [{**obj, "table_name": "assignment_tasks"} for obj in self.invalid_assignment_tasks]
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
                    variant = core_models.LevanteVariant(**variant)
                elif settings.config['INSTANCE'] == 'ROAR':
                    variant = core_models.VariantBase
                else:
                    variant = core_models.VariantBase

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
        for group_id in all_groups:
            try:
                self.valid_user_groups.append(core_models.UserGroup(
                    user_id=user_id,
                    group_id=group_id,
                    is_active=True if group_id in current_groups else False))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_groups.append(
                        {**error, 'id': f"user_id: {user['user_id']}, group_id: {group_id}"})

    def set_user_class(self, user: dict):
        user_id = user.get('user_id', None)
        user_classes = user.get('classes', {})
        all_classes = user_classes.get('all', [])
        current_classes = user_classes.get('current', [])
        for class_id in all_classes:
            try:
                self.valid_user_classes.append(core_models.UserClass(
                    user_id=user_id,
                    class_id=class_id,
                    is_active=True if class_id in current_classes else False))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_classes.append(
                        {**error, 'id': f"user_id: {user['user_id']}, class_id: {class_id}"})

    def set_assignments(self, assignments: list):
        for assignment in assignments:
            assignment_dict = assignment
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    assignment = core_models.AssignmentBase(**assignment)
                elif settings.config['INSTANCE'] == 'ROAR':
                    assignment = core_models.RoarAssignment(**assignment)
                else:
                    assignment = core_models.AssignmentBase(**assignment)
                self.valid_assignments.append(assignment)
                self.set_assignment_task(assignment_dict)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_assignments.append({**error, 'id': assignment['assignment_id']})

    def set_assignment_task(self, assignment: dict):
        assignment_id = assignment.get('assignment_id', None)
        tasks = assignment.get('assessments', [])
        for task in tasks:
            task_id = task.get('taskId', None)
            variant_id = task.get('variantId', None)
            try:
                self.valid_assignment_tasks.append(core_models.AssignmentTask(assignment_id=assignment_id,
                                                                              task_id=task_id,
                                                                              variant_id=variant_id))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_assignment_tasks.append(
                        {**error, 'id': f"assignment_id:{assignment['assignment_id']}, task_id:{task_id}"})

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

    def set_trials(self, user_id: str, run_id: str, trials: list):
        for trial in trials:
            try:
                if settings.config['INSTANCE'] == 'LEVANTE':
                    self.valid_trials.append(core_models.LevanteTrial(**trial))
                elif settings.config['INSTANCE'] == 'ROAR':
                    self.valid_trials.append(core_models.RoarTrial(**trial))
                else:
                    self.valid_trials.append(core_models.TrialBase(**trial))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_trials.append({**error, 'id': f"trial_id: {trial['trial_id']}, run_id: {run_id}, user_id: {user_id}"})

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

    # def set_score_details(self, run: dict):
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

    # def set_values_from_redivis(self, lab_id: str, is_consolidate: bool):
    #     rs = RedivisServices(is_from_firestore=False)
    #     rs.set_dataset(lab_id=lab_id)
    #
    #     self.set_groups(groups=rs.get_tables(table_name="groups"))
    #     self.set_districts(districts=rs.get_tables(table_name="districts"))
    #     self.set_schools(schools=rs.get_tables(table_name="schools"))
    #     self.set_classes(classes=rs.get_tables(table_name="classes"))
    #     self.set_tasks(tasks=rs.get_tables(table_name="tasks"))
    #     self.set_variants(variants=rs.get_tables(table_name="variants"))
    #     self.set_users(users=rs.get_tables(table_name="users"))
    #     self.set_assignments(assignments=rs.get_tables(table_name="assignments"))
    #
    #     runs_table = rs.get_tables(table_name="runs")
    #     if self.valid_users:
    #         for user in self.valid_users:
    #             # logging.info(rs.get_specified_table(table_list=runs_table, spec_key="user_id", spec_value=user.user_id))
    #             self.set_runs(user=user, runs=rs.get_specified_table(table_list=runs_table, spec_key="user_id",
    #                                                                  spec_value=user.user_id))
    #     else:
    #         self.validation_log.append(f"redivis_db has no valid users in {lab_id}.")
    #
    #     trials_table = rs.get_tables(table_name="trials")
    #     if self.valid_runs:
    #         for run in self.valid_runs:
    #             self.set_trials(run=run, trials=rs.get_specified_table(table_list=trials_table, spec_key="run_id",
    #                                                                    spec_value=run.run_id))
    #     else:
    #         self.validation_log.append(f"redivis_db has no valid runs in {lab_id}.")

    # def set_values_for_consolidate(self):
    #     rs = RedivisServices(is_from_firestore=False)
    #
    #     lab_lists = rs.get_datasets_list()
    #     logging.info(lab_lists)
    #     for lab in lab_lists:
    #         self.set_values_from_redivis(lab)
