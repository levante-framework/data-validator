
config = {
    'VERSION': '1.9.12',
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
    'AIRTABLE_FIELD_FIRESTORE_SITE_ID': 'Firestore siteId',
    'AIRTABLE_FIELD_OPEN_ASSIGNMENTS': 'Open Assignments',
    'AIRTABLE_FIELD_REDIVIS_INDIVIDUAL': 'Redivis individual',
    # Airtable column that matches the Redivis dataset name (not Firebase name).
    'AIRTABLE_FIELD_REDIVIS_DATASET_NAME': 'Name',
    # District label for Firestore lookup when Firestore siteId is empty (open-assignments sync).
    'AIRTABLE_FIELD_SITE_NAME': 'Firebase name',
    # Additional fallback column tried when resolving Firestore siteId.
    'AIRTABLE_FIELD_REDIVIS_NAME': 'Redivis name',
    # Date column updated by the redivis_individual_release sync.
    'AIRTABLE_FIELD_VALIDATOR_PIPELINE_DATE': 'validator pipeline date',
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
    # 5xx (e.g. an OOM-killed Cloud Function instance) doesn't lose the day's run.
    'CLOUD_SCHEDULER_RETRY_COUNT': 3,
    'CLOUD_SCHEDULER_RETRY_MAX_DURATION_SECONDS': 1800,
    'CLOUD_SCHEDULER_RETRY_MIN_BACKOFF_SECONDS': 60,
    'CLOUD_SCHEDULER_RETRY_MAX_BACKOFF_SECONDS': 600,
    'CLOUD_SCHEDULER_RETRY_MAX_DOUBLINGS': 3,
    # Attempt deadline (per HTTP attempt). Default for Cloud Scheduler HTTP targets
    # is 180s, max is 1800s. Many sites take longer than 3 min; use the max.
    'CLOUD_SCHEDULER_ATTEMPT_DEADLINE_SECONDS': 1800,
    # data-validator function URL template; {project_id} is filled at runtime.
    'DATA_VALIDATOR_FUNCTION_URL_TEMPLATE': 'https://us-central1-{project_id}.cloudfunctions.net/data-validator',
}
