from pydantic import ValidationError
from core_models import Task, Variant, VariantParams, District, School, Class, User, UserClass, UserAssignment, \
    Assignment, AssignmentTask, Run, Score, Trial
from firestore_services import FirestoreServices
from redivis_services import RedivisServices


class EntityController:

    def __init__(self, lab_id, source):
        self.lab_id = lab_id
        self.source = source
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
        self.valid_variants_params = []
        self.invalid_variants_params = []

        self.valid_users = []
        self.invalid_users = []
        self.valid_user_class = []
        self.invalid_user_class = []
        self.valid_user_assignment = []
        self.invalid_user_assignment = []

        self.valid_assignments = []
        self.invalid_assignments = []
        self.valid_assignment_task = []
        self.invalid_assignment_task = []

        self.valid_runs = []
        self.invalid_runs = []
        # self.valid_score_details = []
        # self.invalid_score_details = []
        self.valid_trials = []
        self.invalid_trials = []

    def set_values_from_firestore(self):
        fs_assessment = FirestoreServices(app_name='assessment_site')
        fs_admin = FirestoreServices(app_name='admin_site')
        self.set_districts(districts=fs_admin.get_districts(lab_id=self.lab_id))
        self.set_schools(schools=fs_admin.get_schools(lab_id=self.lab_id))
        self.set_classes(classes=fs_admin.get_classes(lab_id=self.lab_id))
        self.set_tasks(tasks=fs_assessment.get_tasks())
        self.set_users(users=fs_admin.get_users(lab_id=self.lab_id))
        self.set_assignments(assignments=fs_admin.get_assignments())

        if self.valid_tasks:
            for task in self.valid_tasks:
                self.set_variants(variants=fs_assessment.get_variants(task.task_id))
        else:
            print(f"No valid tasks in {self.lab_id}.")

        if self.valid_users:
            for user in self.valid_users:
                self.set_runs(user=user, runs=fs_assessment.get_runs(user.user_id))
        else:
            print(f"No valid users in {self.lab_id}.")

        if self.valid_runs:
            for run in self.valid_runs:
                self.set_trials(run=run, trials=fs_assessment.get_trials(user_id=run.user_id, run_id=run.run_id,
                                                                         task_id=run.task_id))
        else:
            print(f"No valid runs in {self.lab_id}.")

    def set_values_from_redivis(self, dataset_version):
        rs = RedivisServices(lab_id=self.lab_id, is_from_firestore=False, dataset_version=dataset_version)

        self.set_districts(districts=rs.get_tables(table_name="districts"))
        self.set_schools(schools=rs.get_tables(table_name="schools"))
        self.set_classes(classes=rs.get_tables(table_name="classes"))
        self.set_tasks(tasks=rs.get_tables(table_name="tasks"))
        self.set_variants(variants=rs.get_tables(table_name="variants"))
        self.set_users(users=rs.get_tables(table_name="users"))
        self.set_assignments(assignments=rs.get_tables(table_name="assignments"))

        runs_table = rs.get_tables(table_name="runs")
        # print(runs_table)
        if self.valid_users:
            for user in self.valid_users:
                print(rs.get_specified_table(table_list=runs_table, spec_key="user_id", spec_value=user.user_id))
                self.set_runs(user=user, runs=rs.get_specified_table(table_list=runs_table, spec_key="user_id", spec_value=user.user_id))
        else:
            print(f"No valid users in {self.lab_id}.")

        trials_table = rs.get_tables(table_name="trials")
        if self.valid_runs:
            for run in self.valid_runs:
                self.set_trials(run=run, trials=rs.get_specified_table(table_list=trials_table, spec_key="run_id", spec_value=run.run_id))
        else:
            print(f"No valid runs in {self.lab_id}.")


    def set_districts(self, districts: list):
        for district in districts:
            try:
                district = District(**district)
                self.valid_districts.append(district)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_districts.append({**error, 'district_id': district["district_id"]})

    def set_schools(self, schools: list):
        for school in schools:
            try:
                school = School(**school)
                self.valid_schools.append(school)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_schools.append({**error, 'school_id': school['school_id']})

    def set_classes(self, classes: list):
        for c in classes:
            try:
                c = Class(**c)
                self.valid_classes.append(c)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_classes.append({**error, 'class_id': c['class_id']})

    def set_tasks(self, tasks: list):
        for task in tasks:
            try:
                task = Task(**task)
                self.valid_tasks.append(task)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_tasks.append({**error, 'task_id': task['task_id']})

    def set_variants(self, variants: list):
        for variant in variants:
            try:
                if self.source == "firestore":
                    self.set_variant_params(variant)
                variant = Variant(**variant)
                self.valid_variants.append(variant)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_variants.append({**error, 'variant_id': variant['variant_id']})

    def set_variant_params(self, variant: dict):
        variant_id = variant.get('variant_id', None)
        variant_params = variant.get('params', {})
        for key, value in variant_params.items():
            try:
                self.valid_variants_params.append(VariantParams(
                    variant_id=variant_id,
                    params_field=key,
                    params_type=str(type(value)),
                    params_value=str(value)))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_variants_params.append(
                        {**error, 'variant_id': variant['variant_id'], variant_params: f"{key}, {value}"})

    def set_users(self, users: list):
        for user in users:
            try:
                if self.source == "firestore":
                    self.set_user_class(user)
                    self.set_user_assignment(user)

                user = User(**user)
                self.valid_users.append(user)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_users.append({**error, 'user_id': user['user_id']})

    def set_user_class(self, user: dict):
        user_id = user.get('user_id', None)
        user_classes = user.get('classes', {})
        all_classes = user_classes.get('all', [])
        current_classes = user_classes.get('current', [])
        for class_id in all_classes:
            try:
                self.valid_user_class.append(UserClass(
                    user_id=user_id,
                    class_id=class_id,
                    is_active=True if class_id in current_classes else False))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_user_class.append({**error, 'user_id': user['user_id'], 'class': class_id})

    def set_user_assignment(self, user: dict):
        user_id = user.get('user_id', None)
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
                for error in e.errors():
                    self.invalid_user_assignment.append({**error, 'user_id': user['user_id'], 'assignment': key})

    def set_assignments(self, assignments: list):
        for assignment in assignments:
            try:
                if self.source == "firestore":
                    self.set_assignment_task(assignment)
                assignment = Assignment(**assignment)
                self.valid_assignments.append(assignment)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_assignments.append({**error, 'user_id': assignment['assignment_id']})

    def set_assignment_task(self, assignment: dict):
        assignment_id = assignment.get('assignment_id', None)
        tasks = assignment.get('assessments', [])
        for task in tasks:
            task_id = task.get('taskId', None)
            try:
                self.valid_assignment_task.append(AssignmentTask(
                    assignment_id=assignment_id,
                    task_id=task_id))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_assignment_task.append(
                        {**error, 'assignment_id': assignment['assignment_id'], task: task_id})

    def set_runs(self, user: User, runs: list):
        if self.source == "redivis":
            pass
        for run in runs:
            try:
                self.valid_runs.append(Run(**run))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_assignments.append({**error, 'run_id': run['run_id'], 'user_id': user.user_id})

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

    def set_trials(self, run: Run, trials: list):
        for trial in trials:
            try:
                self.valid_trials.append(Trial(**trial))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_trials.append(
                        {**error, 'run_id': run.run_id, 'user_id': run.user_id, 'trial_id': trial['trial_id']})
