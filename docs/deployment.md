# Video Exclusion Toolbox Deployment

This doc provides information on how to deploy the Video Exclusion Toolbox.

The deployment uses [Terraform](https://www.terraform.io/) to automate the
deployment, and to keep all the Infrastructure as Code (IaC). The files can be
found in the `/terraform/` folder in this repo.

## Roles
The project creates a service account with the following roles:

- `roles/cloudfunctions.invoker`
- `roles/bigquery.jobUser`
- `roles/bigquery.dataEditor`
- `roles/pubsub.publisher`
- `roles/storage.objectAdmin`
- `roles/secretmanager.secretAccessor`
- `roles/cloudbuild.builds.builder`
- `roles/run.invoker`
- `roles/aiplatform.user`

As a user deploying the project, you will require these roles and the following:

- `roles/storage.admin`
- `roles/iam.securityAdmin`

Read more about [Google Cloud roles here](
https://cloud.google.com/iam/docs/understanding-roles).

## OAuth
The project uses OAuth 2.0 scopes and service account roles to manage
permissions. These are the scopes that are required when generating a refresh
token.

```
https://www.googleapis.com/auth/spreadsheets.readonly
https://www.googleapis.com/auth/cloud-platform
https://www.googleapis.com/auth/youtube
https://www.googleapis.com/auth/adwords
https://www.googleapis.com/auth/drive
```

## Pre-requisites

- A new Google Cloud Project
- Appropriate permissions to be able to deploy the project (see [roles](#roles))
- Create a copy of [the template Google Sheet](https://docs.google.com/spreadsheets/d/131Lfxw54bath7bsyhAcMHLG99rKkhL9XSTtMPsnZNTk/copy?usp=sharing&resourcekey=0-FtYQ8IxkFXRNQwim61iQbQ)
  and make a note of the Google Sheet ID (found in the URL after the `/d/`)
- Access to the appropriate Google Ads accounts
- A Google Ads [Developer Token](
  https://developers.google.com/google-ads/api/docs/first-call/dev-token)

## Deployment

### Manual Steps
These changes need to be done once manually, as they are not controlled by
Terraform:

1. If you want to use the sample Looker Studio dashboard to display the data,
request access to
[this dashboard](https://lookerstudio.google.com/c/reporting/2194043e-84bb-432b-b3e6-0553369745be).
The deployment process will prompt you to create a copy of it later on.
Once you've been granted access, proceed with the next step.
This is optional and you can create your own dashboards to connect to the
BigQuery datasets.

1. Open the Google Cloud Project in the UI.

1. Go to [Cloud Storage](https://console.cloud.google.com/storage/browser) and
   create a new bucket, which will be used to keep track of the Terraform state,
   e.g. `my-awesome-project-terraform`. Make a note of the name of the bucket.
   
1. Open the [OAuth Consent Screen](
   https://console.cloud.google.com/apis/credentials/consent) and create a new
   internal app.
1. Open the [API Credentials Screen](
   https://console.cloud.google.com/apis/credentials) -> Create credentials ->
   OAuth Client ID -> Web app -> Set
   `https://developers.google.com/oauthplayground` as an authorised redirect
   URI. Make a note of the `client_id` and the `client_secret`.
1. Open the [OAuth playground](https://developers.google.com/oauthplayground/#step1&scopes=https%3A//www.googleapis.com/auth/spreadsheets.readonly%20https%3A//www.googleapis.com/auth/cloud-platform%20https%3A//www.googleapis.com/auth/youtube%20https%3A//www.googleapis.com/auth/adwords&useDefaultOauthCred=checked),
   and generate a refresh token for the [above scopes](#oauth), using the
   `client_id` and `client_secret` generated in the previous step
1. Open Cloud Shell:
   ![cloud-shell](./images/cloud-shell.png)
1. Enable the APIs in the project by running the following:

   ```
   gcloud services enable \
      serviceusage.googleapis.com \
      cloudresourcemanager.googleapis.com \
      iam.googleapis.com \
      bigquery.googleapis.com \
      googleads.googleapis.com \
      youtube.googleapis.com \
      cloudfunctions.googleapis.com \
      cloudbuild.googleapis.com \
      sheets.googleapis.com \
      cloudscheduler.googleapis.com \
      secretmanager.googleapis.com \
      vision.googleapis.com \
      pubsub.googleapis.com \
      run.googleapis.com \
      eventarc.googleapis.com \
      aiplatform.googleapis.com
   ```

_Side note_: If you're interested in the reason why the APIs aren't controlled
through Terraform, [read this guide](
https://medium.com/rockedscience/how-to-fully-automate-the-deployment-of-google-cloud-platform-projects-with-terraform-16c33f1fb31f).

### Terraform

1. Whilst still in Cloud shell, `git clone` the project, and `cd` into the
   directory.
1. Run the following commands to initialise Terraform:
   
   ```
   cd terraform
   terraform init
   ```
   When prompted, enter the name of the bucket created in step 2 in manual
   steps.

1. Create a file named `terraform.tfvars` and add the following variables:
   
   ```
   project_id = ""
   oauth_refresh_token = ""
   google_cloud_client_id = ""
   google_cloud_client_secret = ""
   google_ads_developer_token = ""
   google_ads_login_customer_id = ""
   config_sheet_id = ""
   ```
   Note that the `google_ads_login_customer_id` is the MCC customer ID in Google
   Ads.

1. Run `terraform plan` and review the proposed changes.
1. Run `terraform apply` to create the infrastructure.
1. The email of the service account created will be output, give view only
   access to the Google sheet containing the config.

By default, the code will be triggered every hour by Cloud Scheduler. You can
run it less often by updating the Cloud Scheduler trigger.
To test everything is working, configure the Google Sheet ([see below](#google-sheet)) and force run the Cloud Scheduler job in the UI.

## Google Sheet

Open your copy of the Google Sheet. This is what you'll be using to configure
the Video Exclusion Toolbox solution.

There are notes in the Sheet that contain instructions for how to set this up.

### Gemini video evaluator tabs

The Gemini video evaluator asks Gemini to look at each video's thumbnails,
title, description and tags. It runs at 10 and 40 minutes past each hour, and
everything about it is set in two tabs of the Config Sheet: `gemini_settings`
and `gemini_categories`. Add the two tabs to your copy of the Sheet if they are
not there yet, and give the tool's service account view access to the Sheet (see
[Terraform](#terraform)).

Both tabs are read by column position, not by header name. The first row of each
tab is taken to be a header and skipped, so always keep a header row at the top:
if it is deleted, the first real row is skipped instead.

Format column B of `gemini_settings` as plain text (Format > Number > Plain
text). Otherwise the Sheet may turn a date typed into it into a date format of
its own, which the tool cannot read.

#### `gemini_settings`

One setting per row: the name in column A, the value in column B. A setting that
is left out, or left blank, takes its default.

| Setting | What it does | Default |
| :--- | :--- | :--- |
| `age_check_mode` | Whether Gemini looks for real children in the thumbnails: `Disabled`, `Shadow` or `Exclude` (see [Modes](#modes)). Cartoons, game avatars, dolls and drawings are not counted. | `Disabled` |
| `max_minor_age` | The oldest age that counts as a child, as a whole number from 1 to 30. A video is flagged if Gemini estimates that the youngest real person in its thumbnails is this age or younger. | `18` |
| `advertiser_context` | A short description of the advertiser or brand, given to Gemini with every video, for example "A spirits brand; content must be suitable for adults only". | None |
| `max_videos_per_batch` | How many videos are sent to Gemini at a time. It can be lowered here, but not raised above the limit set in Terraform (`gemini_video_evaluator_max_videos`, 10000 by default). `0` pauses sending. | The Terraform limit |
| `reevaluate_before` | Sends videos that were last evaluated before this date and time to Gemini again, for example `2026-10-01` or `2026-10-01 14:30`. Times are in UTC, and the value must be in the past. See [When changes take effect](#when-changes-take-effect). | Blank: only videos never evaluated are sent |

#### `gemini_categories`

One content category per row, with a header row of `category`, `mode`,
`min_score` and `description`.

| Column | What it holds |
| :--- | :--- |
| A: `category` | A short name for the category, such as `violence_gore`: 2 to 64 lowercase letters, digits and underscores, starting with a letter. `evaluated` is reserved for the tool's own use. |
| B: `mode` | `Disabled`, `Shadow` or `Exclude` (see [Modes](#modes)). Blank means `Disabled`. |
| C: `min_score` | The score, from `0.50` to `1.00` (or `50%` to `100%`), at which a video counts as belonging to the category. Gemini scores every video from 0.0 (not at all) to 1.0 (definitely). Blank means `0.70`. |
| D: `description` | What the category covers, in plain words. Gemini decides from this text alone, so be specific about what is and is not included. Required unless the mode is `Disabled`. |

For example:

| category | mode | min_score | description |
| :--- | :--- | :--- | :--- |
| violence_gore | Shadow | 0.70 | Real-world graphic violence, street fights, bloodshed, severe injury, torture, or weapon abuse (excluding standard mainstream sports broadcasts). |

#### Modes

*   `Disabled`: Gemini is not asked about it.
*   `Shadow`: Gemini scores it and the scores are saved in the `detection`
    table, but no video is excluded because of it. Use this to check the results
    before switching to `Exclude`.
*   `Exclude`: videos that reach the threshold are added to `VideosToExclude`
    and excluded from your campaigns.

#### When changes take effect

Switching between `Shadow`, `Exclude` and `Disabled`, changing a `min_score`, or
changing `max_minor_age` takes effect on the next run for every video that
already has a score. Gemini is not asked again, so this costs nothing.

A change to what Gemini is asked only affects videos sent to Gemini after the
change. That covers adding a category, switching a check on from `Disabled`, and
editing a description or the `advertiser_context`. To apply such a change to
videos evaluated earlier, set `reevaluate_before` to the date and time of the
change. Those videos are then sent again a batch at a time, after any videos
never evaluated, and each costs the same as a new evaluation. Clear the cell to
stop at any point; videos already sent again keep their new results.

#### Removing a category

Set its mode to `Disabled`, or delete its row. Either way, from the next run it
no longer adds videos to `VideosToExclude`. `Disabled` keeps the description in
the Sheet for later.

Its earlier scores stay in the `detection` table, and its rule stays in the
`detection_policy` table, switched off. If you add the category back under the
same name, the rule follows the Sheet again: in `Shadow` or `Exclude` it applies
to the category's earlier scores as well as new ones.

#### If the Sheet has a problem

If either tab holds a value the tool cannot use, the run stops before sending
any new videos or changing any exclusion rule, and lists every problem it found,
each with how to fix it. The list appears in the logs of the
`vet-gemini-video-evaluator` function, and Cloud Scheduler shows the run as
failed. Results of batches that were already running are still saved.

#### For administrators

*   On every run the tool writes one row per check to the `detection_policy`
    table, with an id starting `first_party_gemini_` and priority 50. Changes
    made directly to those rows are overwritten on the next run. To override
    them, add a row with a different id and a higher priority.
*   The tool treats every `detection_policy` row whose id starts
    `first_party_gemini_` as its own. Such a row that the Sheet no longer
    produces is switched off (`enabled = FALSE`, with the date in `notes`),
    never deleted. Do not give rows you add by hand an id with that start.
*   Cloud Scheduler calls the function with an empty JSON body, `{}`. The
    function takes no parameters, and rejects a request that has any, because
    every setting is read from the Sheet.
*   For empirical throughput, latency, infrastructure sizing (`4Gi` RAM, `2`
    vCPU, `900s` timeout), token consumption, unit economics (`~$1.95 / 1,000`
    videos at the default `MEDIA_RESOLUTION_MEDIUM`), and resolution quality
    benchmarks, see
    [Gemini Video Evaluator — Performance, Cost, Sizing & Resolution Benchmarks](./gemini-video-evaluator-benchmarks.md).

## Disclaimers
__This is not an officially supported Google product.__

Copyright 2023 Google LLC. This solution, including any related sample code or
data, is made available on an “as is,” “as available,” and “with all faults”
basis, solely for illustrative purposes, and without warranty or representation
of any kind. This solution is experimental, unsupported and provided solely for
your convenience. Your use of it is subject to your agreements with Google, as
applicable, and may constitute a beta feature as defined under those agreements.
To the extent that you make any data available to Google in connection with your
use of the solution, you represent and warrant that you have all necessary and
appropriate rights, consents and permissions to permit Google to use and process
that data. By using any portion of this solution, you acknowledge, assume and
accept all risks, known and unknown, associated with its usage, including with
respect to your deployment of any portion of this solution in your systems, or
usage in connection with your business, if at all.