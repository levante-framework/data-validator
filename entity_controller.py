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

        self.valid_sites = []
        self.invalid_sites = []
        self.valid_cohorts = []
        self.invalid_cohorts = []
        self.valid_administrations = []
        self.invalid_administrations = []
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

        self.valid_surveys = []
        self.invalid_surveys = []
        self.valid_survey_responses = []
        self.invalid_survey_responses = []
        self.survey_responses_stats = {"student": 0, "teacher": 0, "caregiver": 0}

        self.valid_user_sites = []
        self.valid_user_cohorts = []
        self.valid_user_schools = []
        self.valid_user_classes = []

        self.invalid_user_sites = []
        self.invalid_user_cohorts = []
        self.invalid_user_schools = []
        self.invalid_user_classes = []

        self._user_org_maps = {}

        self.valid_user_administrations = []
        self.invalid_user_administrations = []

    def adding_schema_row_to_data(self):
        """
        Ensure every table has at least one row: the 'schema_row'.
        If a table already has rows, the schema_row is appended (last).
        """
        now = datetime.now(timezone.utc)
        for table_name, (attr, model_cls) in utils.schema_registry().items():
            seq = getattr(self, attr, None)
            if seq is None:
                setattr(self, attr, [])
                seq = getattr(self, attr)
            # Always append a schema row (even if first/only row)
            try:
                seq.append(model_cls.schema_row(now=now))
            except Exception as e:
                logging.warning(f"schema_row append failed for {table_name} ({attr}): {e}")

    def validate_data_from_firestore(self):
        self.process_users()
        if self.valid_users:
            if not self.org.is_guest:
                self.process_surveys()
            self.process_runs()
            if self.valid_runs:
                self.process_trials()
                self.process_tasks_variants()

        # Determine whether it's using guest.
        if not self.org.is_guest:
            if self.org.filters.org_filter.key == 'districts':
                self.process_sites()
                self.process_cohorts()
                self.process_schools()
                self.process_classes()
            elif self.org.filters.org_filter.key == 'groups':
                self.process_cohorts()

            self.process_administration()

        # self.adding_schema_row_to_data()

        # Track schema
        # for task in self.valid_tasks:
        #     if task.task_id == 'survey':
        #         for survey_type, schema_dict in self.survey_key_usage.items():
        #             fs.upload_task_schema_to_firestore(dict_type=survey_type, schema_usage=self.survey_key_usage,
        #                                                task_id=task.task_id, new_schemas=self.new_schemas['surveys'])
        #     else:
        #         fs.upload_task_schema_to_firestore(dict_type='runKeys', schema_usage=self.run_key_usage,
        #                                            task_id=task.task_id, new_schemas=self.new_schemas['runs'])
        #         fs.upload_task_schema_to_firestore(dict_type='trialKeys', schema_usage=self.trial_key_usage,
        #                                            task_id=task.task_id, new_schemas=self.new_schemas['trials'])

    def get_validated_data(self):
        data = {}
        for table_name, (attr, _model_cls) in utils.schema_registry().items():
            data[table_name] = [obj.model_dump() for obj in getattr(self, attr, [])]
        invalid_data = self.get_invalid_data()
        if invalid_data:
            data["invalid_data"] = invalid_data
        return data

    def get_invalid_data(self):
        invalid_rows: list[dict] = []

        def normalize_item(item):
            # You already store dicts like {"id": "...", "errors": e.errors()}.
            # Keep dicts as-is; coerce everything else to a simple message.
            if isinstance(item, dict):
                return item
            try:
                return {"error": str(item)}
            except Exception:
                return {"error": "unknown invalid item"}

        registry = utils.schema_registry() if hasattr(self, "_schema_registry") else {}

        for table_name, (valid_attr, _model_cls) in registry.items():
            # Derive invalid attr: valid_users -> invalid_users, valid_user_sites -> invalid_user_sites, etc.
            inv_attr = None
            if isinstance(valid_attr, str) and valid_attr.startswith("valid_"):
                inv_attr = "invalid_" + valid_attr[len("valid_"):]
            if not inv_attr or not hasattr(self, inv_attr):
                continue

            items = getattr(self, inv_attr, None) or []
            for item in items:
                row = normalize_item(item)
                # Don’t override if caller already set table_name
                row.setdefault("table_name", table_name)
                invalid_rows.append(row)

        for invalid_item in invalid_rows:
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

        return invalid_rows

    def process_sites(self):
        logging.info("Now Validating Sites...")
        site_ids = sorted({x.site_id for x in self.valid_user_sites})
        sites = fs.get_org_by_org_id_list(org_name="site", org_id_list=list(site_ids))

        self.set_sites(sites=sites)

    def process_cohorts(self):
        logging.info("Now Validating Cohorts...")

        cohort_ids = sorted({x.cohort_id for x in self.valid_user_cohorts})
        cohorts = fs.get_org_by_org_id_list(org_name="cohort", org_id_list=list(cohort_ids))

        self.set_cohorts(cohorts=cohorts)

    def process_schools(self):
        logging.info("Now Validating Schools...")

        school_ids = sorted({x.school_id for x in self.valid_user_schools})

        schools = fs.get_org_by_org_id_list(org_name="school", org_id_list=list(school_ids))

        self.set_schools(schools=schools)

    def process_classes(self):
        logging.info("Now Validating Classes...")

        class_ids = sorted({x.class_id for x in self.valid_user_classes})

        classes = fs.get_org_by_org_id_list(org_name="class", org_id_list=list(class_ids))

        self.set_classes(classes=classes)

    def process_administration(self):
        """
        Must be called AFTER users are processed, since we now
        derive administrations from the user_assignments table.
        """
        admin_ids = {ua.administration_id for ua in self.valid_user_administrations}
        administrations = fs.get_administrations_by_ids(list(admin_ids))
        self.set_administrations(administrations=administrations)

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
        logging.info("Now Validating Users...")

        users = fs.get_users(is_guest=self.org.is_guest,
                             date_filter=self.org.filters.date_filter,
                             org_filter=self.org.filters.org_filter,
                             user_filter=self.org.filters.user_filter)
        self.set_users(users=users)

    def process_surveys(self):
        logging.info("Now Validating Surveys...")
        for user in self.valid_users:
            surveys, survey_responses = fs.get_surveys(user_id=user.user_id,
                                                       user_type=user.user_type,
                                                       date_filter=self.org.filters.date_filter,
                                                       survey_key_usage=self.survey_key_usage)

            if surveys:
                self.set_surveys(user=user, surveys=surveys)

            if survey_responses:
                self.set_survey_responses(user=user, survey_responses=survey_responses)

            # Stats: count a user if they have at least one survey instance
            if surveys:
                if user.user_type == 'student':
                    self.survey_responses_stats["student"] += 1
                elif user.user_type == 'teacher':
                    self.survey_responses_stats["teacher"] += 1
                elif user.user_type == 'parent':
                    self.survey_responses_stats["caregiver"] += 1

    def process_runs(self):
        logging.info("Now Validating Runs...")
        for user in self.valid_users:
            self.set_runs(user=user, runs=fs.get_runs(user_id=user.user_id,
                                                      run_key_usage=self.run_key_usage,
                                                      is_guest=self.org.is_guest,
                                                      date_filter=self.org.filters.date_filter))

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

    def set_cohorts(self, cohorts: list):
        for cohort in cohorts:
            try:
                cohort = core_models.CohortBase(**cohort)
                self.valid_cohorts.append(cohort)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_cohorts.append({**error, 'id': cohort["cohort_id"]})

    def set_sites(self, sites: list):
        for site in sites:
            try:
                site = core_models.SiteBase(**site)
                self.valid_sites.append(site)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_sites.append({**error, 'id': site["site_id"]})

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

    def set_users(self, users: list[dict]):
        for raw in users:
            # keep original dict for assignment extraction
            user_dict = dict(raw)
            uid = user_dict.get('user_id') or user_dict.get('uid')
            try:
                if settings.config.get('INSTANCE') == 'LEVANTE':
                    user_model = core_models.LevanteUser(**user_dict)
                else:
                    user_model = core_models.UserBase(**user_dict)
                self.valid_users.append(user_model)
            except ValidationError as e:
                for err in e.errors():
                    self.invalid_users.append({**err, 'id': uid})

            # Build user_assignments from the user doc
            self.set_user_administrations(user_dict)
            self.process_user_org_joins(user_dict)

    def set_user_administrations(self, user: dict):
        user_id = user.get('user_id') or user.get('uid')
        if not user_id:
            return

        assigned_map = user.get('assignments_assigned') or {}
        started_map = user.get('assignments_started') or {}
        completed_map = user.get('assignments_completed') or {}

        for administration_id, assigned_payload in assigned_map.items():
            try:
                ua = core_models.UserAdministration(
                    user_id=user_id,
                    administration_id=administration_id,
                    date_assigned=assigned_payload,
                    date_started=started_map.get(administration_id),
                    is_completed=(administration_id in completed_map),
                )
                self.valid_user_administrations.append(ua)
            except ValidationError as e:
                for err in e.errors():
                    self.invalid_user_administrations.append(
                        {**err, 'id': f"{user_id}:{administration_id}"}
                    )

    def process_user_org_joins(self, user: dict):
        user_id = user.get('user_id') or user.get('uid')
        if not user_id:
            return

        sites = user.get("districts", {})
        cohorts = user.get("groups", {})
        schools = user.get("schools", {})
        classes = user.get("classes", {})

        for site_id, is_active in utils.ids_with_active(org_map=sites):
            try:
                self.valid_user_sites.append(core_models.UserSite(
                    user_id=user_id, site_id=site_id, is_active=is_active
                ))
            except ValidationError as e:
                self.invalid_user_sites.append({'id': f'{user_id}:{site_id}', 'errors': e.errors()})

        for cohort_id, is_active in utils.ids_with_active(org_map=cohorts):
            try:
                self.valid_user_cohorts.append(core_models.UserCohort(
                    user_id=user_id, cohort_id=cohort_id, is_active=is_active
                ))
            except ValidationError as e:
                self.invalid_user_cohorts.append({'id': f'{user_id}:{cohort_id}', 'errors': e.errors()})

        for school_id, is_active in utils.ids_with_active(org_map=schools):
            try:
                self.valid_user_schools.append(core_models.UserSchool(
                    user_id=user_id, school_id=school_id, is_active=is_active
                ))
            except ValidationError as e:
                self.invalid_user_schools.append({'id': f'{user_id}:{school_id}', 'errors': e.errors()})

        for class_id, is_active in utils.ids_with_active(org_map=classes):
            try:
                self.valid_user_classes.append(core_models.UserClass(
                    user_id=user_id, class_id=class_id, is_active=is_active
                ))
            except ValidationError as e:
                self.invalid_user_classes.append({'id': f'{user_id}:{class_id}', 'errors': e.errors()})

    def set_surveys(self, user: core_models.LevanteUser, surveys: list):
        for survey in surveys:
            try:
                self.valid_surveys.append(core_models.Survey(**survey))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_surveys.append(
                        {**error, 'id': f"user_id: {user.user_id}, survey_id: {survey.get('survey_id')}"})

    def set_survey_responses(self, user: core_models.LevanteUser, survey_responses: list):
        for survey_response in survey_responses:
            try:
                self.valid_survey_responses.append(core_models.SurveyResponse(**survey_response))
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_survey_responses.append(
                        {**error, 'id': f"user_id: {user.user_id}, survey_id: {survey_response.get('survey_id')}"})

    def set_administrations(self, administrations: list):
        for administration in administrations:
            try:
                administration = core_models.AdministrationBase(**administration)
                self.valid_administrations.append(administration)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_administrations.append({**error, 'id': administration['administration_id']})

    def set_runs(self, user: core_models.LevanteUser, runs: list):
        for run in runs:
            try:
                run_model = core_models.LevanteRun(**run)
                # remove intro runs.
                if run_model.task_id == "intro":
                    continue
                run_model.add_age_from_users(birth_year=user.birth_year, birth_month=user.birth_month)
                self.valid_runs.append(run_model)
            except ValidationError as e:
                for error in e.errors():
                    self.invalid_runs.append(
                        {**error, 'id': f"run_id: {run['run_id']}, user_id: {user.user_id}"})

    def set_trials(self, run: core_models.RunBase, trials: list):
        for trial in trials:
            try:
                trial_model = core_models.LevanteTrial(**trial)
                # remove instruction and training
                if ("instruction" in str(trial_model.assessment_stage or "").lower() or
                        "display" in str(trial_model.trial_mode or "").lower() or
                        "training" in str(trial_model.corpus_trial_type or "").lower()):
                    continue
                is_test_trial = (trial_model.assessment_stage == 'test_response' or
                                 (trial_model.is_practice_trial is not None and not trial_model.is_practice_trial))
                # Execute action based on the condition
                if is_test_trial:
                    run.add_non_practice_trials(trial_model)

                self.valid_trials.append(trial_model)

            except ValidationError as e:
                for error in e.errors():
                    self.invalid_trials.append(
                        {**error,
                         'id': f"trial_id: {trial['trial_id']}, run_id: {run.run_id}, user_id: {run.user_id}, task_id:{trial.get('task_id', None)}"})
