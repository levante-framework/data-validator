
config = {
    'VERSION': '1.9.32',
    'INSTANCE': 'LEVANTE',
    'EXTERNAL_DATA_BUCKET_NAME': 'levante-external-data',
    'ADMIN_SERVICE_ACCOUNT_SECRET_ID': 'adminServiceAccount',
    'VALIDATOR_API_SECRET_ID': 'validatorApiKey',
    'REDIVIS_API_TOKEN_SECRET_ID': 'firebaseRedivisPipelineAccessToken',
    'REDIVIS_IDENTITY_ACCOUNT_SECRET_ID': 'redivisIdentityEmailAccount',
    'SLACK_NOTIFICATION_WEB_HOOK': 'slackNotificationWebHook',
    # Optional: Secret Manager id for a second Incoming Webhook (e.g. admin-only dry_run).
    # Leave empty to use SLACK_NOTIFICATION_WEB_HOOK for all Slack notifications.
    'SLACK_ADMIN_WEBHOOK_SECRET_ID': '',
    # Optional: Secret Manager id for the weekly-report channel webhook. If the
    # secret is missing the weekly report falls back to SLACK_NOTIFICATION_WEB_HOOK.
    'SLACK_WEEKLY_REPORT_WEBHOOK_SECRET_ID': 'slackWeeklyReportWebHook',
    # LEVANTE Entities — Dataset table (field names + base/table ids)
    'AIRTABLE_API_TOKEN_SECRET_ID': 'airtableTokenUpdateEntitiesDatasets',
    'AIRTABLE_LEVANTE_ENTITIES_BASE_ID': 'appIDUfcKdekzTiIJ',
    'AIRTABLE_DATASET_TABLE_ID': 'tblu4NwcVZX9MbuWK',
    # Rows that define multi-site "special" raw datasets and their query scopes.
    'AIRTABLE_SPECIAL_DATASET_TABLE_ID': 'tbllc141VVhJfsKNa',
    'AIRTABLE_SPECIAL_FIELD_DATASET_NAME': 'dataset_name',
    'AIRTABLE_SPECIAL_FIELD_DATASET_REF_ID': 'dataset_ref_id',
    'AIRTABLE_SPECIAL_FIELD_PROCESSED_NAME': 'processed_name',
    'AIRTABLE_SPECIAL_FIELD_PROCESSED_REF_ID': 'processed_ref_id',
    'AIRTABLE_SPECIAL_FIELD_START_DATE': 'start_date',
    'AIRTABLE_SPECIAL_FIELD_END_DATE': 'end_date',
    'AIRTABLE_SPECIAL_FIELD_DATASET_LINK': 'dataset_link',
    'AIRTABLE_SPECIAL_FIELD_ORG_ID': 'org_id',
    'AIRTABLE_SPECIAL_FIELD_USER_LIMIT': 'user_limit',
    'AIRTABLE_SPECIAL_FIELD_IS_GUEST': 'is_guest',
    'AIRTABLE_SPECIAL_FIELD_USER_FILTER': 'user_filter',
    'AIRTABLE_FIELD_FIRESTORE_SITE_ID': 'Firestore siteId',
    'AIRTABLE_FIELD_OPEN_ASSIGNMENTS': 'Open Assignments',
    'AIRTABLE_FIELD_REDIVIS_INDIVIDUAL': 'Redivis individual',
    # Airtable ``Name`` = processed (unmarked) Redivis dataset name.
    'AIRTABLE_FIELD_REDIVIS_DATASET_NAME': 'Name',
    # District label for Firestore lookup when Firestore siteId is empty (open-assignments sync).
    'AIRTABLE_FIELD_SITE_NAME': 'Firebase name',
    # Airtable ``Redivis name`` = raw Redivis dataset name (``{Name}-raw``).
    'AIRTABLE_FIELD_REDIVIS_NAME': 'Redivis name',
    # Persistent Redivis referenceIds (4-char) for raw / processed datasets.
    'AIRTABLE_FIELD_DATASET_REF_ID': 'dataset_ref_id',
    'AIRTABLE_FIELD_PROCESSED_REF_ID': 'processed_ref_id',
    # Raw companion suffix: validator uploads to ``{Name}-raw``; processed stays unmarked.
    'RAW_DATASET_SUFFIX': '-raw',
    # Date column set once when the per-site validator scheduler is first provisioned.
    'AIRTABLE_FIELD_VALIDATOR_PIPELINE_DATE': 'validator pipeline setup date',
    # Date column updated after a successful process_dataset run for the unmarked processed dataset.
    'AIRTABLE_FIELD_PROCESSED_DATASET_LAST_UPDATE': 'Redivis processed dataset last update',
    # Levante Redivis workflow that fills the unmarked processed dataset from raw.
    'REDIVIS_PROCESS_WORKFLOW_USER': 'levante',
    'REDIVIS_PROCESS_WORKFLOW_NAME': 'process_dataset:zr0v',
    'REDIVIS_PROCESS_NOTEBOOK_NAME': 'process_dataset',
    # Shared notebook: per-site jobs may hit "Notebook is already running". Wait/retry
    # until free (total budget), with exponential backoff between attempts.
    'REDIVIS_PROCESS_BUSY_RETRY_MAX_SECONDS': 3600,
    'REDIVIS_PROCESS_BUSY_RETRY_INITIAL_SECONDS': 30,
    'REDIVIS_PROCESS_BUSY_RETRY_MAX_SLEEP_SECONDS': 120,
    'REDIVIS_PROCESS_BUSY_POLL_SECONDS': 30,
    # Placeholder written into Firestore siteId when no Firestore district matches.
    'MISSING_SITE_ID_PLACEHOLDER': 'missing_site_id',
    # Cloud Scheduler config used to provision daily data-validator jobs per site.
    'CLOUD_SCHEDULER_REGION': 'us-central1',
    'CLOUD_SCHEDULER_TIMEZONE': 'America/Los_Angeles',
    # Daily cron schedule. Computed from hour + (base_minute + stagger offset).
    # When window > 0, each dataset gets a deterministic minute offset in
    # [base_minute, base_minute + window) — same dataset_id always picks the
    # same slot, so re-running redivis_release doesn't reshuffle schedules.
    # Set window = 0 to disable stagger (every job fires at the same minute).
    'CLOUD_SCHEDULER_HOUR': 12,
    'CLOUD_SCHEDULER_BASE_MINUTE': 0,
    'CLOUD_SCHEDULER_STAGGER_WINDOW_MINUTES': 30,
    'CLOUD_SCHEDULER_JOB_PREFIX': '',
    # Retry behavior applied to newly-created scheduler jobs so a single transient
    # API failure doesn't lose the day's run.
    'CLOUD_SCHEDULER_RETRY_COUNT': 3,
    'CLOUD_SCHEDULER_RETRY_MAX_DURATION_SECONDS': 1800,
    'CLOUD_SCHEDULER_RETRY_MIN_BACKOFF_SECONDS': 60,
    'CLOUD_SCHEDULER_RETRY_MAX_BACKOFF_SECONDS': 600,
    'CLOUD_SCHEDULER_RETRY_MAX_DOUBLINGS': 3,
    # Attempt deadline (per HTTP attempt to the Run Job API). The validation itself
    # runs asynchronously in Cloud Run Jobs (up to CLOUD_RUN_JOB_TASK_TIMEOUT_SECONDS).
    'CLOUD_SCHEDULER_ATTEMPT_DEADLINE_SECONDS': 1800,
    # Service account Cloud Scheduler uses for OAuth when calling the Run Job API.
    # Must have permission to run the job (roles/run.developer or run.jobs.run).
    # Empty → {project_id}@appspot.gserviceaccount.com
    'CLOUD_SCHEDULER_OAUTH_SERVICE_ACCOUNT': '',
    # Cloud Run Job (sole runtime for validation and auxiliary operations).
    'CLOUD_RUN_JOB_NAME': 'data-validator',
    'CLOUD_RUN_JOB_REGION': 'us-central1',
    'CLOUD_RUN_JOB_TASK_TIMEOUT_SECONDS': 86400,
    # Thin HTTP service that accepts clean JSON + API-Key and starts the job (202).
    'CLOUD_RUN_TRIGGER_SERVICE_NAME': 'data-validator-trigger',
}
