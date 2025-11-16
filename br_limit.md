1️⃣ **IMMEDIATE ANSWER:**
Your Bedrock Batch design must respect *per-model TPS limits, per-model token-per-minute limits, and per-job shard-size/input-size restrictions* to avoid throttling or job failures.

---

2️⃣ **THE APPROACH (Bedrock limits that directly impact your batch pipeline):**

### **(A) Model Throughput Limits (TPS / TPM)**

**What they are:**
Each Bedrock model (Claude 3.5, Llama 3, etc.) enforces **Transactions-Per-Second (TPS)** and **Tokens-Per-Minute (TPM)** limits.

**Why it matters:**
If your batch creates too many parallel shards, Bedrock throttles the job, slowing it down or extending job duration.

**Typical limits (varies by account):**

* **TPS:** usually 1–20 TPS default per model
* **TPM (total tokens per minute):** model-dependent, often **300K–3M tokens/min**
* **Daily token quotas:** often **10–50M tokens/day** unless raised

**Impact to your pipeline:**
200k prompts × (10k input + 2k output) ≈ **2.4B tokens/run**, which exceeds default quotas.
→ You **must request a quota increase or run batch jobs in multiple days**.

---

### **(B) Bedrock Batch Job Input Constraints**

Your batch pipeline must follow the limits for a single Bedrock Batch job:

1. **Max input file size (per JSONL shard):**

   * Typical: **≤ 50 MB per shard file**
   * You must partition prompts accordingly (your design uses 500–2000 per shard = safe).

2. **Max number of input files per batch job:**

   * Common: **≤ 5,000 files** per job
   * Your 200k prompts with 1k-per-shard → ~200 shards → safe.

3. **Max jobs in RUNNING state:**

   * Typically: **≤ 10 concurrent jobs per account/model**

**Why it matters:**
Oversized shard files or too many shards can cause job rejection.

---

### **(C) Max Output Size Limits**

Each response file must comply with:

* **Object size limit:** typically **≤ 50 MB**
* **Per-record output token limit**: depends on model context length
  (e.g., Claude 3.5 Sonnet: ~200k context)

Your output (2k tokens per prompt) is safe.

---

### **(D) Context Window Limits**

Each model enforces:

* **Max input + output tokens** per prompt
* Example: Claude 3.5 Sonnet = **200k tokens limit**

Your prompt = 10k input + 2k output = **12k total** → far below context limit.

---

### **(E) S3 Throughput & Prefix Limitations**

Batch mode reads/writes thousands of small files.

Constraints to consider:

* S3 recommends **≥ 3K PUTs/s** and **≥ 3K GETs/s per prefix**
* Use **prefix sharding** to avoid internal listing bottlenecks
  Example: `s3://…/input/run_id/shard=00001/`

---

### **(F) IAM and Role Assumption Limits**

Bedrock Batch needs:

* **IAM PassRole** permission
* **Role session duration ≤ 1h or ≤12h** depending on model provider

If the session expires mid-job, output fails.

---

3️⃣ **ADVANCED OPTIMIZATION**

* **Ask AWS to raise your per-model TPM limit** to at least
  `2.4B tokens / (desired duration in minutes)`
  Example: If you want a 2-hour batch window:
  → need ~20M tokens/minute.

* If quota raise is slow, **split 200k prompts into multiple jobs** across hours or days.

* Use **multiple models** (Claude + Llama 3 + Titan) to parallelize quota consumption.

* Compress prompt text by:

  * removing unused fields
  * aggressive formatting
  * caching dedupe (your fingerprint design helps)
    This reduces token cost and speeds up batch execution.

* Log Bedrock throttling from job status; if `ThrottlingException` or `InsufficientThroughputException` appears, reduce shard concurrency.

---

4️⃣ **TROUBLESHOOTING**

| Issue                   | Cause                                       | Fix                                              |
| ----------------------- | ------------------------------------------- | ------------------------------------------------ |
| **Job takes very long** | TPM limit reached                           | Request quota increase, reduce shards per minute |
| **Job rejected**        | Too many input files or shard sizes > 50 MB | Re-tune SHARD_SIZE to ~1000 prompts/shard        |
| **Partial output only** | Output file exceeds 50 MB                   | Reduce prompts per shard                         |
| **Throttle exceptions** | TPS too low for parallelism                 | Increase TPS quota or stagger shard ingestion    |
| **S3 listing slow**     | Too many objects under one prefix           | Use prefix sharding (`/shard=00001/`)            |
| **IAM role errors**     | Missing PassRole or wrong trust             | Fix Bedrock execution role trust policy          |

---

If you want, I can calculate **exact optimal shard size** for your dataset using your 10k input / 2k output token spec and typical model limits — just tell me your Bedrock region and target model (Claude Sonnet, Llama 3, Titan, etc.).
