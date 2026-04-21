
config = {
    'VERSION': '1.7.0',
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
}
