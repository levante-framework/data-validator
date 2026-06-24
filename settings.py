
config = {
    'VERSION': '1.5.4',
    'INSTANCE': 'LEVANTE',
    'EXTERNAL_DATA_BUCKET_NAME': 'levante-external-data',
    'ADMIN_SERVICE_ACCOUNT_SECRET_ID': 'adminServiceAccount',
    'ASSESSMENT_SERVICE_ACCOUNT_SECRET_ID': 'assessmentServiceAccount',
    'VALIDATOR_API_SECRET_ID': 'validatorApiKey',
    'REDIVIS_API_TOKEN_SECRET_ID': 'firebaseRedivisPipelineAccessToken',
    'REDIVIS_IDENTITY_ACCOUNT_SECRET_ID': 'redivisIdentityEmailAccount',
    'SLACK_NOTIFICATION_WEB_HOOK': 'slackNotificationWebHook',

    # Run-reliability thresholds.
    # Minimum number of non-practice test trials for a (fixed-form) run to be valid.
    'TEST_TRIALS_MIN': 10,
    # Adaptive (CAT) runs may legitimately stop early once a target standard error
    # is reached. A completed adaptive run whose theta SE is at or below this value
    # is treated as valid even with fewer than TEST_TRIALS_MIN trials.
    # Set to None to disable the SE ceiling (accept any present SE).
    'ADAPTIVE_THETA_SE_MAX': 0.5,
}
