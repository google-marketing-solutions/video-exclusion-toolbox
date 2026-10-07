# Gemini Video Evaluator — Performance, Cost, Sizing & Resolution Benchmarks

This document provides empirical benchmarks for the `vet-gemini-video-evaluator`
service (`gemini-3.8-flash` via Agent Platform Batch Inference), covering
throughput, step-by-step latency, infrastructure sizing, token consumption, unit
economics, and the `mediaResolution` quality benchmark.

For setup and configuration instructions, see the
[deployment guide](./deployment.md#gemini-video-evaluator-tabs).

---

## 1. Load Test & Multi-Day Soak Summary

Over a ~70-hour continuous evaluation run against an `~8M`-row (`~7.5M` distinct
active videos) `youtube_video` corpus (`europe-west2` compute and BigQuery,
`global` Agent Platform Batch Inference), `vet-gemini-video-evaluator` evaluated
**`~850,000` distinct videos** (`~5.1M` active rows written to
`video_exclusion_toolbox.detection`) across progressive batch sizes (`500`,
`5,000`, and `50,000` videos/batch) followed by an unattended ~70-hour soak at
`5,000` videos/batch (`10,40 * * * *`).

### 1.1 Reliability & Pipeline Health (`140+` Scheduled Soak Runs)

| Metric | ~70-Hour Soak (`5,000` videos/batch) | Combined Total (incl. `500` / `5k` / `50k` Load Tests) |
| :--- | ---: | ---: |
| **Scheduled HTTP Invocations (`200 OK`)** | **`100.0%`** | **`100.0%`** |
| **Failed Telemetry Steps / Unhandled Errors** | `0` | `0` |
| **Leaked Temporary Tables (`_gemini_eval_*`)** | `0` | `0` |
| **Ticks Dispatched vs. `IN_FLIGHT_THROTTLED`** | `~98%` dispatched / `~2%` throttled | `~96%` dispatched / `~4%` throttled |
| **Videos Evaluated (`audit.evaluated` rows)** | `~690,000` reconciled | **`~850,000`** reconciled |
| **Row-Level Evaluation Success Rate** | **`99.993%`** | **`99.994%`** |
| **Row-Level Evaluation Failures** | `~0.007%` | **`~0.006%`** — see [§1.4](#14-failure-mode-breakdown-0006) |

### 1.2 Step-by-Step Latency & Scaling Profile by Batch Size

All measurements below were recorded on Cloud Run Gen 2 (`2 vCPU`, `4 GiB RAM`,
`timeout_seconds = 900`, Cloud Scheduler `attempt_deadline = 960s`):

| Pipeline Step | `500` Videos / Batch (`2,000` URLs) | `5,000` Videos / Batch (`20,000` URLs) | `50,000` Videos / Batch (`200,000` URLs) | Dominant Scaling Factor |
| :--- | ---: | ---: | ---: | :--- |
| **`RECONCILE_BATCHES`** (Phase 1) | `~4.5 s` (`3,000` rows) | **`35.5 s`** avg (`6.7–100.5 s`, `30,000` rows) | **`94.7 s`** (`~300,000` rows) | Reading `_gemini_eval_output_*`, streaming Newline-Delimited JSON (NDJSON) staging, and executing Data Manipulation Language (DML) `MERGE` into `detection`. |
| **`READ_CONFIG`** | `2.5 s` | **`2.7 s`** avg (`1.8–4.9 s`) | `2.6 s` | Drive-backed external table reads (`gemini_settings`, `gemini_categories`) — `O(1)`. |
| **`SYNC_POLICIES`** | `3.5 s` | **`3.8 s`** avg (`2.7–7.3 s`) | `3.7 s` | DML `MERGE` into `detection_policy` — `O(1)`. |
| **`DISCOVER_CANDIDATES`** | `9.7 s` | **`7.1 s`** avg (`5.3–11.8 s`) | **`26.3 s`** (`24.4–28.1 s`) | Full-corpus deduplication of `youtube_video` (`~8M` rows) anti-joined against `detection` and `VideosToExclude`. |
| **`PROBE_THUMBNAILS`** | `7.2 s` (`~277` URLs/s) | **`35.2 s`** avg (`28.7–55.8 s`, `~568` URLs/s) | **`349.9 s`** (`347.5–352.3 s`, `~571` URLs/s) | Concurrent HTTP `GET` pool (`32` workers) against `https://i.ytimg.com/vi/{id}/hq*.jpg` — linear `O(N)` at **`~570 URLs/sec`** (`~142 videos/sec`). |
| **`STAGE_BATCH_INPUT`** | `1.8 s` | **`5.7 s`** avg (`4.3–10.1 s`) | **`30.4 s`** (`28.8–31.9 s`) | Building JSON requests in memory and streaming NDJSON to `_gemini_eval_input_*`. |
| **`DISPATCH_BATCH_JOB`** | `1.2 s` | **`1.3 s`** avg (`0.8–5.1 s`) | `1.4 s` | `genai_client.batches.create()` call — `O(1)`. |
| **Total Cloud Run Wall Time** | **`~31.9 s`** | **`~89.4 s`** avg (`~55–155 s`) | **`~435–510 s`** (`7.3–8.5 min`) | Fits within `300s` timeout for `≤ 5,000` videos; requires `900s` timeout for `50,000` videos. |
| **Agent Platform Batch Turnaround** | **`~6–8 min`** | **`~7–18 min`** (`~98%` finish `< 30 min`) | **`~15–22 min`** | Asynchronous server-side execution (`$0` Cloud Run compute while running). |

> [!IMPORTANT]
> **Infrastructure Sizing (`512Mi` Out-of-Memory at `50,000` Videos):**
> At `50,000` videos/batch, materializing `50,000` candidate dictionaries,
> `200,000` probe URL tasks, and `50,000` serialized JSON request payloads
> (`~100 MB` raw NDJSON) plus streaming `300,000` `DetectionRecord` objects
> during reconciliation exceeds `512 MiB` RAM (`552 MiB` peak usage).
> Provisioning **`4Gi` RAM, `2` vCPU, `900s` function timeout, and `960s` Cloud
> Scheduler `attempt_deadline`** (configured in
> [`resource_google_cloudfunctions2_function.tf`](../terraform/resource_google_cloudfunctions2_function.tf)
> and
> [`resource_google_cloud_scheduler_job.tf`](../terraform/resource_google_cloud_scheduler_job.tf))
> provides `~7x` memory headroom and `~1.8x` timeout headroom at the
> `50,000`-video ceiling.

### 1.3 Representative Detection Yield (`~850,000` Evaluated Videos)

| Rule / Policy (`detection_policy`) | Taxonomy in `detection` | Threshold | Share of Evaluated Corpus | Marginal / Exclusive Contribution |
| :--- | :--- | :---: | ---: | :--- |
| **`first_party_gemini_minor_presence`** | `age_eval.minor_presence` | `max_minor_age <= 17` (`score >= 1.0`) | **`~5.2%`** | **`~91%` of minor hits** are flagged *exclusively* by visual thumbnail age detection and score `< 0.70` across all 4 content categories. |
| **`first_party_gemini_content_kids`** | `content_eval.kids` | `score >= 0.70` | **`~1.5%`** | Catches animated cartoons, nursery rhymes, toys, and video-game roleplay where no real human face appears (`is_real_human = false`). |
| **`first_party_gemini_content_brand_safety_unsuitable`** | `content_eval.brand_safety_unsuitable` | `score >= 0.70` | **`~0.7%`** | Overlaps with child-directed and youth-oriented content unsuitable for age-restricted or regulated brands. |
| **`first_party_gemini_content_violence_gore`** | `content_eval.violence_gore` | `score >= 0.70` | **`~0.05%`** | Graphic real-world violence, street fights, severe injury, or weapon abuse. |
| **`first_party_gemini_content_political_extremism`** | `content_eval.political_extremism` | `score >= 0.70` | **`~0.02%`** | Extremist propaganda, sectarian incitement, or acute disinformation. |
| **Combined Union (Any Policy Hit)** | — | — | **`~6.9%`** | Net new videos identified beyond the existing keyword blocklist (`VideosToExclude` anti-join). |

### 1.4 Failure Mode Breakdown (`~0.006%`)

| Failure Class (`audit.evaluated` with `$.status = 'failed'`) | Share of Total Corpus | Root Cause & Remediation |
| :--- | ---: | :--- |
| **Transient Remote Image-Fetch Errors** | `~0.005%` | Remote image fetcher timed out or exhausted buffers fetching a verified `i.ytimg.com` URL during burst batch execution. **Remediation:** Bounded retry (`MAX_FAILED_ATTEMPTS = 3`) in [`sql.py`](../src/gemini_video_evaluator/utils/sql.py) and [`batch.py`](../src/gemini_video_evaluator/utils/batch.py). |
| **Empty Model Candidate (`response contained no text`)** | `~0.001%` | Safety/recitation filter or empty candidate returned by Gemini on specific video metadata/thumbnails. Bounded retry (`MAX_FAILED_ATTEMPTS = 3`) re-tests up to 3 times before permanently marking evaluated. |

---

## 2. Measured Token Footprint & Unit Economics (`gemini-3.8-flash`)

### 2.1 Measured Token Breakdown (`n = 5,000` Production Videos)

Direct inspection of `response.usageMetadata` and `promptTokensDetails` across a
`5,000`-video batch under `MEDIA_RESOLUTION_HIGH`:

| Video Cohort | Videos (`n=5,000`) | Share | Image Tokens (`promptTokensDetails`) | Text & Schema Tokens | Total Input (`promptTokenCount`) | Total Output (`candidates` + `thoughts`) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **4 Verified Thumbnails (`hqdefault`, `hq1`, `hq2`, `hq3`)** | `4,603` | `92.06%` | **`4,256.0`** (`1,064` / image) | `1,628.2` | **`5,884.2`** | **`323.1`** (`323.0` + `0.1`) |
| **0 Verified Thumbnails (Text-Only Fallback)** | `397` | `7.94%` | `0.0` | `1,560.6` | **`1,560.6`** | **`232.7`** (`198.8` + `33.9`) |
| **Blended Corpus Average** | **`5,000`** | **`100.0%`** | **`3,918.1`** (`70.7%` of input) | **`1,622.8`** (`29.3%` of input) | **`5,540.94`** | **`315.84`** (`313.10` + `2.74`) |

### 2.2 Image Tokenization by `mediaResolution` on `gemini-3.8-flash`

Gemini's vision tokenizer (`generationConfig.mediaResolution`) supports three
resolution levels. Empirical measurement on `gemini-3.8-flash` across `499`
4-thumbnail videos (`480x360` `hq*` JPEGs) established the exact token counts
billed per image:

- **`MEDIA_RESOLUTION_HIGH` (API default when omitted):** Applies 4x adaptive
  tiling (**`1,064` tokens/image**, or **`4,256` image tokens for 4
  thumbnails**).
- **`MEDIA_RESOLUTION_MEDIUM` (Configured default in
  [`prompt.py`](../src/gemini_video_evaluator/utils/prompt.py)):** Applies 2x
  intermediate tiling (**`540` tokens/image**, or **`2,160` image tokens for 4
  thumbnails** — a **`49.2%` reduction in image tokens**).
- **`MEDIA_RESOLUTION_LOW`:** Encodes each `480x360` thumbnail as a single
  uncropped vision tile (**`266` tokens/image**, or **`1,064` image tokens for 4
  thumbnails** — a **`75.0%` reduction in image tokens**).

### 2.3 Measured Cost Table (`HIGH` vs. `MEDIUM` vs. `LOW` on `gemini-3.8-flash`)

Using `gemini-3.8-flash` Agent Platform Batch Inference list pricing (50% batch
discount: **`$0.375 / 1M` input tokens**, **`$1.875 / 1M` output tokens**) plus
measured BigQuery on-demand compute (**`$0.009` / 1k videos**) and Cloud Run Gen
2 (`2 vCPU / 4 GiB` = **`$0.001` / 1k videos**), blended across the corpus mix
(`~92%` 4-thumbnail videos + `~8%` 0-thumbnail videos):

| Resolution Mode (`generationConfig.mediaResolution`) | Measured Tokens / Image | Image Tokens / 4-Thumb Video | Blended Input Tokens / Video | Output Tokens / Video | Cost per `1,000` Videos | Cost per `5,000` Batch | Daily Cost at `500` Videos / 30m (`24k`/day) | Cost per `1M` Videos | Cost per `5M` Videos |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`MEDIA_RESOLUTION_HIGH`** | `1,064` | `4,256` | `5,541` | `316` | **`$2.68`** | **`$13.40`** | **`$64.32` / day** | **`$2,680`** | **`$13,400`** |
| **`MEDIA_RESOLUTION_MEDIUM` *(Default)*** | `540` | `2,160` | `3,611` | `311` | **`$1.95`** (`-27.3%`) | **`$9.74`** | **`$46.75` / day** | **`$1,948`** | **`$9,740`** |
| **`MEDIA_RESOLUTION_LOW`** | `266` | `1,064` | `2,602` | `310` | **`$1.57`** (`-41.4%`) | **`$7.85`** | **`$37.68` / day** | **`$1,570`** | **`$7,850`** |

---

## 3. Experimental Design: Image Tokenization Resolution Quality Benchmark

To verify whether lowering `mediaResolution` from `MEDIA_RESOLUTION_HIGH` to
`MEDIA_RESOLUTION_MEDIUM` or `MEDIA_RESOLUTION_LOW` affects classification
quality—specifically visual minor age estimation and cartoon/human
discrimination—we executed a 3-arm stratified benchmark on `500` videos (`499`
completed across all arms; `1` video was excluded because its YouTube thumbnails
were deleted between runs).

### 3.1 Stratified `500`-Video Benchmark Cohort

A uniform random sample is `~93%` easy negatives (`score = 0.0`), which masks
degradation on hard visual boundary cases. Instead, we drew a deterministic
**`500`-video stratified cohort** (`FARM_FINGERPRINT(video_id)`, all requiring
`verified_thumbnail_count = 4` in `audit.evaluated`):

| Stratum | Sampled (`N=500`) | Paired (`N=499`) | Selection Criteria from Existing `HIGH_0` Detections | Failure Mode Stress-Tested |
| :--- | ---: | ---: | :--- | :--- |
| **1a. Visual Minors — Pre-Teen (`0–12`)** | `75` | `75` | `minor_presence = 1.0`, `min_real_human_age <= 12` | **False Negatives (Missed Children):** Does lower resolution miss infants or pre-teen children (`0–12`)? |
| **1b. Visual Minors — Adolescent (`13–17`)** | `75` | `75` | `minor_presence = 1.0`, `min_real_human_age BETWEEN 13 AND 17` (`26` aged `13–15`, `49` aged `16–17`) | **Boundary Recall (`13–17` → `18+`):** Does facial texture smoothing make 13–17 year-olds look 18+? |
| **2. Borderline Young Adults (`18–22`)** | `100` | `99` | `minor_presence = 0.0`, `min_real_human_age BETWEEN 18 AND 22` | **False Positives (`18–22` → `<= 17`):** Does resolution loss blur young adult faces into false minor exclusions? |
| **3. Non-Human / Cartoon / Avatar Faces** | `50` | `50` | `minor_presence = 0.0`, `min_real_human_age IS NULL`, `person_count > 0` | **Avatar Confusion (`is_real_human` flip):** Does lower resolution make 3D game avatars (Roblox, Minecraft, Sims) or anime characters look like real humans? |
| **4a–4d. Positive Content Category Hits** | `100` | `100` | `25` videos each with `score >= 0.70` on `kids`, `brand_safety_unsuitable`, `violence_gore`, and `political_extremism` (without minors) | **Multimodal Category Drift:** Does lower thumbnail resolution change category scores (`0.0–1.0`) or flip `>= 0.70` decisions? |
| **5a–5b. Clean True Negatives (`25+` & No-Person)** | `100` | `100` | `50` videos with `min_real_human_age >= 25` + `50` videos with `0` persons, all category scores `< 0.30` | **Baseline Specificity:** Confirms zero false-positive inflation on standard adult and non-person placements. |

### 3.2 Experimental Arms & Control for Run-to-Run Non-Determinism

Even at `temperature = 0.0`, models exhibit small run-to-run variance on
borderline visual judgments (e.g., estimating age `17` vs. `18`). To separate
baseline run-to-run variance from true resolution degradation, we evaluated all
`499` paired videos across 4 sets of predictions:

1. **`HIGH_0` (Initial Baseline):** Original `MEDIA_RESOLUTION_HIGH` evaluation
   stored in `detection`.
2. **`HIGH_1` (Same-Time Control Repeat):** Fresh `MEDIA_RESOLUTION_HIGH`
   evaluation (`1,064` tokens/image). Comparing `HIGH_1` vs. `HIGH_0` measures
   the **test-retest repeatability ceiling**; comparing `MEDIUM` and `LOW`
   directly against `HIGH_1` eliminates selection-boundary regression to the
   mean.
3. **`MEDIUM`:** Fresh `MEDIA_RESOLUTION_MEDIUM` evaluation (`540`
   tokens/image).
4. **`LOW`:** Fresh `MEDIA_RESOLUTION_LOW` evaluation (`266` tokens/image).

---

## 4. Empirical Results (`N = 499` Paired 4-Thumbnail Videos, `1,497` Evaluations)

### 4.1 Token Consumption & Latency (`gemini-3.8-flash`)

| Arm (`generationConfig.mediaResolution`) | Paired Videos (`N`) | Image Tokens / Thumbnail | Image Tokens / Video (4 Thumbs) | Mean Text Tokens / Video | Mean Total Input Tokens / Video | Mean Output Tokens / Video | Mean Online Latency / Video | Blended Cost / `1k` Videos |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`MEDIA_RESOLUTION_HIGH` (`HIGH_1`)** | `499` | **`1,064.0`** | **`4,256.0`** | `1,573.7` | `5,829.7` | `307.8` | `6.29s` | **`$2.68`** (baseline) |
| **`MEDIA_RESOLUTION_MEDIUM` (`MEDIUM`)** | `499` | **`540.0`** | **`2,160.0`** (`-49.2%`) | `1,573.7` | `3,733.7` (`-36.0%`) | `302.2` | `5.60s` (`-11.0%`) | **`$1.95`** (**`-27.3%`**) |
| **`MEDIA_RESOLUTION_LOW` (`LOW`)** | `499` | **`266.0`** | **`1,064.0`** (`-75.0%`) | `1,573.7` | `2,637.7` (`-54.8%`) | `301.8` | `5.96s` (`-5.2%`) | **`$1.57`** (**`-41.4%`**) |

### 4.2 Task A: Minor-Presence Detection & Age Estimation Quality

#### A1. Direct Comparison Against Same-Time Control (`HIGH_1`) and Initial Baseline (`HIGH_0`)

| Metric | `HIGH_1` vs. `HIGH_0` *(Test-Retest Ceiling)* | `MEDIUM` vs. `HIGH_1` *(Direct Resolution Delta)* | `LOW` vs. `HIGH_1` *(Direct Resolution Delta)* | `MEDIUM` vs. `HIGH_0` *(vs. Initial Baseline)* | `LOW` vs. `HIGH_0` *(vs. Initial Baseline)* | Verdict (`MEDIUM` vs. `LOW`) |
| :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| **Overall `minor_presence` Agreement (`N=499`)** | **`97.60%`** (`487/499`) | **`97.19%`** (`485/499`) | `96.79%` (`483/499`) | `96.39%` (`481/499`) | `95.19%` (`475/499`) | `MEDIUM` is within **`0.41 pp`** of `HIGH_1` ceiling |
| **Overall `minor_presence` Cohen's κ** | **`0.9417`** | **`0.9293`** | `0.9184` | `0.9112` | `0.8806` | `MEDIUM` Δκ = `-0.012`; `LOW` Δκ = `-0.061` vs. `HIGH_0` |
| **Stratum 1a Recall: Pre-Teen `0–12` (`n=75`)** | **`97.33%`** (`73/75`)\* | **`95.89%`** (`70/73`) | **`98.63%`** (`72/73`) | `93.33%` (`70/75`) | `96.00%` (`72/75`) | Both preserve high sensitivity on young children |
| **Stratum 1b Recall: All Teens `13–17` (`n=75`)** | **`88.00%`** (`66/75`) | **`89.39%`** (`59/66`)\*\* | **`80.30%`** (`53/66`) | `82.67%` (`62/75`) | **`73.33%`** (`55/75`) | **`LOW` loses `-14.7 pp` recall on teens vs. `HIGH_1`** |
| **— Sub-Stratum: Young Teens `13–15` (`n=26`)** | **`96.15%`** (`25/26`) | **`92.31%`** (`24/26`) | **`80.77%`** (`21/26`) | `92.31%` (`24/26`) | **`80.77%`** (`21/26`) | **`MEDIUM` differs by 1 video (`24` vs. `25`); `LOW` drops to `80.8%`** |
| **— Sub-Stratum: Borderline `16–17` (`n=49`)** | **`83.67%`** (`41/49`) | **`77.55%`** (`38/49`) | **`69.39%`** (`34/49`) | `77.55%` (`38/49`) | **`69.39%`** (`34/49`) | **`LOW` misses `30.6%` of 16–17yo videos** |
| **Crowded-Frame Minor Recall (`>=3` persons)** | **`91.34%`** (`116/127`) | **`93.97%`** (`109/116`) | `90.52%` (`105/116`) | `88.19%` (`112/127`) | `84.25%` (`107/127`) | `MEDIUM` preserves multi-person minor detection |
| **Stratum 2 False-Positive Rate (`18–22`, `n=99`)** | **`1.01%`** (`1/99`) | **`0.00%`** (`0/99`) | `0.00%` (`0/99`) | **`0.00%`** (`0/99`) | `1.01%` (`1/99`) | Zero false-positive inflation on young adults |
| **Stratum 3 Avatar False-Minor Rate (`n=50`)** | **`0.00%`** (`0/50`) | **`0.00%`** (`0/50`) | **`0.00%`** (`0/50`) | **`0.00%`** (`0/50`) | **`0.00%`** (`0/50`) | Zero cartoon/avatar false minor exclusions |
| **Stratum 4 & 5 False-Minor Rate (`n=200`)** | **`0.00%`** (`0/200`) | **`0.00%`** (`0/200`) | **`0.00%`** (`0/200`) | **`0.00%`** (`0/200`) | **`0.00%`** (`0/200`) | `100%` specificity on adult and non-person videos |

*\*Note on Stratum 1a (`0–12`): Two videos where `HIGH_0` had inferred a child
in a small background inset were scored as adults (`35` and `26` years old)
across all three repeat runs (`HIGH_1`, `MEDIUM`, and `LOW`).*
*\*\*Note on Stratum 1b (`13–17`): In addition to matching `59 / 66` of
`HIGH_1`'s minor detections, `MEDIUM` also flagged `3` adolescent videos from
`HIGH_0` where `HIGH_1` drifted to `18–20`, bringing `MEDIUM` to `62 / 75`
(`82.7%`) vs. `66 / 75` (`88.0%`) for `HIGH_1`.*

#### A2. Minimum Real-Human Age Estimation Accuracy & Bias (`N = 393–395` Videos with Real Humans)

| Comparison | Mean Absolute Error (All Humans) | Signed Bias Δage (All Humans) | 90th Percentile \|Δage\| | Mean Absolute Error (Minors ≤ 17) | Signed Bias Δage (Minors ≤ 17) | Mean Detected Persons / Video | Mean Real Humans / Video |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`HIGH_1` vs. `HIGH_0` *(Repeatability Ceiling)*** | **`0.706` yrs** | `+0.256` yrs | `2` yrs | **`0.867` yrs** | `+0.533` yrs | `3.385` (vs. `3.387`) | `2.900` (vs. `2.884`) |
| **`MEDIUM` vs. `HIGH_1` *(Direct Control)*** | **`0.936` yrs** | **`-0.025` yrs** | **`2` yrs** | **`0.835` yrs** | **`+0.403` yrs** | `3.307` (vs. `3.385`) | `2.838` (vs. `2.900`) |
| **`LOW` vs. `HIGH_1` *(Direct Control)*** | `1.028` yrs | `+0.253` yrs | `2` yrs | `0.964` yrs | `+0.547` yrs | `3.275` (vs. `3.385`) | `2.802` (vs. `2.900`) |
| **`MEDIUM` vs. `HIGH_0` *(Initial Baseline)*** | **`1.056` yrs** | `+0.232` yrs | **`2` yrs** | `1.302` yrs | `+0.846` yrs | `3.307` (vs. `3.387`) | `2.838` (vs. `2.884`) |
| **`LOW` vs. `HIGH_0` *(Initial Baseline)*** | `1.216` yrs | `+0.505` yrs | **`3` yrs** | **`1.383` yrs** | **`+1.007` yrs** | `3.275` (vs. `3.387`) | `2.802` (vs. `2.884`) |

> [!WARNING]
> **Why `MEDIA_RESOLUTION_LOW` Degrades on Adolescents (`13–17`):**
> At `266` tokens/image (`LOW`), smoothing of facial skin texture introduces a
> **`+1.007`-year upward age bias on minors** relative to `HIGH_0`
> (90th-percentile error increases from `2` years to `3` years). Adolescent
> subjects aged `13–15` that both `HIGH_1` and `MEDIUM` classify as `12–16`
> years old are frequently over-estimated as `18–20` years old under `LOW`,
> causing `13–15` recall to drop from `96.2%` (`HIGH_1`) and `92.3%` (`MEDIUM`)
> down to **`80.8%`** (`LOW`).

#### A3. Threshold Sensitivity Analysis (`max_minor_age = 17` vs. `18`)

Because `detection` stores the raw `$.min_real_human_age` in `evidence` and
`build_recompute_age_scores_sql` in
[`sql.py`](../src/gemini_video_evaluator/utils/sql.py) recomputes
`minor_presence` in place whenever `max_minor_age` changes in the Config Sheet
(without re-calling Gemini), we evaluated how `max_minor_age = 18` compares to
`17`:

| Arm & Threshold | Stratum 1a Recall (`0–12`, `n=75`) | Sub-Stratum Recall (`13–15`, `n=26`) | Sub-Stratum Recall (`16–17`, `n=49`) | Young Adult `18–22` False-Positive Rate (`n=99`) | Clean Adult / Other False-Positive Rate (`n=250`) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`HIGH_1` (`age <= 17`)** | `97.3%` (`73/75`) | `96.2%` (`25/26`) | `83.7%` (`41/49`) | **`1.0%`** (`1/99`) | **`0.0%`** (`0/250`) |
| **`HIGH_1` (`age <= 18`)** | `97.3%` (`73/75`) | `100.0%` (`26/26`) | `93.9%` (`46/49`) | `5.1%` (`5/99`) | **`0.0%`** (`0/250`) |
| **`MEDIUM` (`age <= 17`)** | `93.3%` (`70/75`) | **`92.3%`** (`24/26`) | `77.6%` (`38/49`) | **`0.0%`** (`0/99`) | **`0.0%`** (`0/250`) |
| **`MEDIUM` (`age <= 18`)** | `94.7%` (`71/75`) | **`96.2%`** (`25/26`) | **`89.8%`** (`44/49`) | **`3.0%`** (`3/99`) | **`0.0%`** (`0/250`) |
| **`LOW` (`age <= 17`)** | `96.0%` (`72/75`) | `80.8%` (`21/26`) | `69.4%` (`34/49`) | `1.0%` (`1/99`) | **`0.0%`** (`0/250`) |
| **`LOW` (`age <= 18`)** | `96.0%` (`72/75`) | `88.5%` (`23/26`) | `85.7%` (`42/49`) | `6.1%` (`6/99`) | **`0.0%`** (`0/250`) |

### 4.3 Task B: Multimodal Content Category Suitability (`N = 499` Videos)

In the table below, Score MAD denotes Mean Absolute Difference across continuous
`[0.0, 1.0]` scores:

| Category (`min_score = 0.70`) | Comparison Pair | Score Pearson `r` | Score MAD | Threshold Agreement (`>= 0.70`) | Cohen's κ | Ref Positives / Test Positives (`TP / FP / FN`) |
| :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| **`kids`** | `HIGH_1` vs. `HIGH_0` | `0.9689` | `0.0298` | **`98.00%`** | `0.8503` | `38` / `34` (`31 TP, 3 FP, 7 FN`) |
| | `MEDIUM` vs. `HIGH_1` | **`0.9575`** | **`0.0353`** | **`96.99%`** | `0.7600` | `34` / `33` (`26 TP, 7 FP, 8 FN`) |
| | `LOW` vs. `HIGH_1` | `0.9526` | `0.0379` | `97.39%` | `0.7861` | `34` / `31` (`26 TP, 5 FP, 8 FN`) |
| **`brand_safety_unsuitable`** | `HIGH_1` vs. `HIGH_0` | `0.9400` | `0.0389` | **`95.99%`** | `0.8136` | `68` / `54` (`51 TP, 3 FP, 17 FN`) |
| | `MEDIUM` vs. `HIGH_1` | **`0.9490`** | **`0.0385`** | **`95.59%`** | `0.7639` | `54` / `50` (`41 TP, 9 FP, 13 FN`) |
| | `LOW` vs. `HIGH_1` | `0.9417` | `0.0408` | `96.39%` | `0.8035` | `54` / `48` (`42 TP, 6 FP, 12 FN`) |
| **`violence_gore`** | `HIGH_1` vs. `HIGH_0` | `0.9423` | `0.0380` | **`98.00%`** | `0.7518` | `25` / `17` (`16 TP, 1 FP, 9 FN`) |
| | `MEDIUM` vs. `HIGH_1` | **`0.9244`** | **`0.0439`** | **`99.00%`** | **`0.8336`** | `17` / `14` (`13 TP, 1 FP, 4 FN`) |
| | `LOW` vs. `HIGH_1` | `0.9268` | `0.0442` | `98.60%` | `0.7806` | `17` / `16` (`13 TP, 3 FP, 4 FN`) |
| **`political_extremism`** | `HIGH_1` vs. `HIGH_0` | `0.9243` | `0.0393` | **`98.60%`** | `0.8301` | `25` / `18` (`18 TP, 0 FP, 7 FN`) |
| | `MEDIUM` vs. `HIGH_1` | **`0.9018`** | **`0.0462`** | **`99.20%`** | **`0.8847`** | `18` / `18` (`16 TP, 2 FP, 2 FN`) |
| | `LOW` vs. `HIGH_1` | `0.8878` | `0.0489` | `98.80%` | `0.8271` | `18` / `18` (`15 TP, 3 FP, 3 FN`) |

### 4.4 End-to-End Video Exclusion Decision Agreement (`minor_presence == 1.0` OR Any Category `>= 0.70`)

| Comparison Pair | Ref Excluded | Test Excluded | True Pos (`TP`) | True Neg (`TN`) | False Pos (`FP`) | False Neg (`FN`) | Overall Decision Agreement | Cohen's κ | Exclusion Recall | Exclusion Specificity |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **`HIGH_1` vs. `HIGH_0` *(Test-Retest Ceiling)*** | `254` | `221` | `219` | `243` | `2` | `35` | **`92.59%`** | **`0.8520`** | **`86.22%`** | `99.18%` |
| **`MEDIUM` vs. `HIGH_1` *(Direct Control)*** | `221` | `209` | `196` | `265` | `13` | `25` | **`92.38%`** | **`0.8448`** | **`88.69%`** | `95.32%` |
| **`LOW` vs. `HIGH_1` *(Direct Control)*** | `221` | `203` | `192` | `267` | `11` | `29` | `91.98%` | `0.8362` | `86.88%` | `96.04%` |
| **`MEDIUM` vs. `HIGH_0` *(Initial Baseline)*** | `254` | `209` | `207` | `243` | `2` | `47` | `90.18%` | `0.8042` | `81.50%` | `99.18%` |
| **`LOW` vs. `HIGH_0` *(Initial Baseline)*** | `254` | `203` | `200` | `242` | `3` | `54` | `88.58%` | `0.7723` | `78.74%` | `98.78%` |

### 4.5 Engineering Conclusion & Default Configuration

1. **Reject `MEDIA_RESOLUTION_LOW` (`266` tokens/image):**
   - Although `MEDIA_RESOLUTION_LOW` cuts cost by `41.4%` (`$2.68` → `$1.57 / 1k`
     videos) and performs well on young children (`0–12`) and text-supported
     categories, it causes a **tangible degradation on adolescent (`13–17`)
     visual age estimation**, introducing a `+1.01`-year upward age bias that
     reduces recall on `13–15` year-olds from `96.2%` (`HIGH_1`) to `80.8%` and
     on `16–17` year-olds from `83.7%` to `69.4%`.
2. **Default to `MEDIA_RESOLUTION_MEDIUM` (`540` tokens/image) with `DEFAULT_MAX_MINOR_AGE = 18`:**
   - Against the same-time `HIGH_1` control, `MEDIA_RESOLUTION_MEDIUM` achieves
     **`97.19%` binary agreement on `minor_presence`** (κ = `0.9293`, vs.
     `97.60%` / κ = `0.9417` for `HIGH_1` test-retest), **`92.38%` end-to-end
     exclusion agreement** (κ = `0.8448`, vs. `92.59%` / κ = `0.8520` for
     `HIGH_1` test-retest), **near-zero systematic age bias (`-0.025` years vs.
     `HIGH_1`)**, **`0.0%` false-positive rate across all adult and avatar
     strata (`0 / 299`)**, and **`93.97%` crowded-frame minor recall**.
   - Setting `MEDIA_RESOLUTION = 'MEDIA_RESOLUTION_MEDIUM'` in
     [`prompt.py`](../src/gemini_video_evaluator/utils/prompt.py) reduces image
     token consumption by **`49.2%`** (`4,256` → `2,160` image tokens/video) and
     reduces total end-to-end evaluation cost by **`27.3%`** (**`$2.68` →
     `$1.95` per `1,000` videos**).
   - Setting `DEFAULT_MAX_MINOR_AGE = 18` in
     [`config.py`](../src/gemini_video_evaluator/utils/config.py) achieves
     **`96.2%`** (`25/26`) recall on `13–15` year-olds and **`89.8%`** (`44/49`)
     recall on `16–17` year-olds under `MEDIA_RESOLUTION_MEDIUM` at a `3.0%`
     (`3/99`) false-positive rate on `18–22` young adults and `0.0%` (`0/250`)
     on `25+` adults.

---

## Disclaimers

**This is not an officially supported Google product.**

Copyright 2026 Google LLC. This solution, including any related sample code or
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
