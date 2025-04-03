resource "time_sleep" "wait_60_seconds_after_role_assignment" {
  create_duration = "60s"
  depends_on = [
    resource.google_project_iam_member.cloud_functions_invoker,
    resource.google_project_iam_member.bigquery_job_user,
    resource.google_project_iam_member.bigquery_data_owner,
    resource.google_project_iam_member.pubsub_publisher,
    resource.google_project_iam_member.storage_object_admin,
    resource.google_project_iam_member.secret_accessor,
    resource.google_project_iam_member.cloudbuild_builds_builder
  ]
}