## Data flow (Mermaid)

Ниже — диаграмма Mermaid, показывающая поток данных от raw источников через bronze/silver слои к `cc_reports`.

```mermaid
flowchart TD
  subgraph Raw_Sources["Raw sources / APIs / Streams"]
    ZENDESK[Zendesk API]
    SUPABASE[Supabase DB]
    CHARGEBEE_API[Chargebee API]
    APPFOLLOW_API[AppFollow API]
    TRUSTPILOT_API[Trustpilot API]
    FIREHOSE["Firehose / Kinesis (Typeform, Payrails, Paddle)"]
    AMPLITUDE[Amplitude events]
  end

  subgraph Bronze["Bronze (Iceberg / Athena)"]
    B_ZENDESK["data_bronze_zendesk_prod.*<br>(zendesk_audit, zendesk_tickets, zendesk_csat)"]
    B_SUPABASE["data_bronze_supabase_prod.*<br>(smartyme_public__*, auth__users, crm_*)"]
    B_CHARGEBEE["data_bronze_chargebee_prod.*<br>(transaction, subscription, customer, ...)"]
    B_AMPLITUDE["data_bronze_amplitude_prod.*"]
    B_FIREHOSE["firehose_* (typeform, payrails, paddle, paddle_webhook)"]
  end

  subgraph Silver["Silver / Versioned / Aggregates"]
    S_PRODUCT["data_silver_product_sessions_prod.*<br>(ff_purchase_sessions, sf_purchase_sessions)"]
    S_APPFOLLOW["data_silver_appfollow_prod.appfollow_reviews"]
    S_TRUSTPILOT["data_silver_trustpilot_prod.trustpilot_reviews"]
    LOOKUPS["chargebee_product_catalog_2 / data_lookups_*"]
  end

  subgraph Reports["Final reports / cc_reports"]
    REPORTS["customer-care-analytics/cc_reports<br>([actual] cc report - overall.sql, voc_tags_logic.sql,<br>main_subs_and_upsell.sql, users_scopes.sql, ... )<br>reads data_bronze_*/data_silver_*"]
    QCSV["customer-care-analytics/query_results (CSV snapshots)"]
  end

  %% Ingest edges (DAG -> Bronze/Silver)
  ZENDESK -->|"zendesk_pipeline"| B_ZENDESK
  SUPABASE -->|"supabase_pipeline"| B_SUPABASE
  CHARGEBEE_API -->|"chargebee_bronze_pipeline"| B_CHARGEBEE
  AMPLITUDE -->|"amplitude_pipeline"| B_AMPLITUDE
  FIREHOSE -->|"firehose/kinesis ingestion"| B_FIREHOSE
  APPFOLLOW_API -->|"appfollow_pipeline"| S_APPFOLLOW
  TRUSTPILOT_API -->|"trustpilot_pipeline"| S_TRUSTPILOT

  %% Bronze -> Silver / transformations
  B_SUPABASE -->|"used by / joined"| S_PRODUCT
  B_CHARGEBEE -->|"joined/enriched"| S_PRODUCT
  B_FIREHOSE -->|"typeform -> voc extraction"| S_TRUSTPILOT
  B_ZENDESK -->|"enrichments / csat"| LOOKUPS

  %% Silver -> Reports
  S_PRODUCT -->|"read by"| REPORTS
  S_APPFOLLOW -->|"read by (voc)"| REPORTS
  S_TRUSTPILOT -->|"read by (voc)"| REPORTS
  B_ZENDESK -->|"read by (tickets, audit)"| REPORTS
  B_SUPABASE -->|"read by (profiles/subscriptions)"| REPORTS
  B_CHARGEBEE -->|"read by (transactions/subs)"| REPORTS
  B_FIREHOSE -->|"read by (payments/typeform snippets)"| REPORTS

  %% local snapshots
  QCSV -.->|"local samples used for dev"| REPORTS

  %% annotate key DAG files
  classDef dagfile fill:#f8f9fa,stroke:#333,stroke-width:1px;
  ZENDESK:::dagfile
  SUPABASE:::dagfile
  CHARGEBEE_API:::dagfile
  APPFOLLOW_API:::dagfile
  TRUSTPILOT_API:::dagfile

  %% Legend (compact)
  subgraph Legend[" "]
    A1["DAG names -> producing tables (examples):"]
    A2["dp-dags/src/dags/zendesk_pipeline/zendesk_pipeline.py -> data_bronze_zendesk_*"]
    A3["dp-dags/src/dags/supabase_pipeline/supabase_pipeline.py -> data_bronze_supabase_*"]
    A4["dp-dags/src/dags/product_sessions_pipeline -> data_silver_product_sessions_*"]
    A5["dp-dags/src/dags/appfollow_pipeline/appfollow_pipeline.py -> data_silver_appfollow_*"]
    A6["dp-dags/src/dags/trustpilot_pipeline/trustpilot_pipeline.py -> data_silver_trustpilot_*"]
  end
```
```

---

Файл сгенерирован автоматически — напиши, если надо экспортировать в PNG/SVG.
