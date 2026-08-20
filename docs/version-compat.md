# NiFi 2.7.2 vs 1.24.0 — property difference map

*Generated 2026-08-19 by `make version-map`, from a live property-descriptor harvest of both servers. Do not edit by hand — regenerate instead.*

You author flows against the 2.x catalog; work runs 1.24/1.28. Between
those lines Apache renamed most property **keys** to their display names,
added properties that do not exist on 1.x, and dropped others. A 2.x-only
key pushed at a 1.x server does **not** error — NiFi stores it as an inert
*dynamic* property while the real property keeps its default, so the
processor quietly does the wrong thing. That silence is what this map ends.

## What niflow does with it

* `niflow validate FILE` — offline, no server needed. Checks the flow
  against the declared **compatibility baseline** (`NIFLOW_MIN_NIFI_VERSION`
  in `.niflow.env`, default 1.24) with no flag, and **exits non-zero**
  if the flow sets a property that cannot land there. It fails rather than
  warns on purpose: on the server that failure is silent, and a warning in
  a wall of output is how this class of bug reaches production.
  `--target-version 2.7.2` checks some other line instead;
  `--no-compat-check` (or a baseline of `none`) turns it off.
* `niflow plan` / `niflow push` — the same check runs automatically against
  the live server's own version and logs every affected component **before**
  the first mutation. It warns, it does not block: NiFi accepts the flow, and
  that acceptance is precisely the problem. Pushing to a *different* line
  from the baseline (a 2.x server, say) is legitimate and is never blocked —
  but a flow that would not survive the baseline is called out there too.
* `niflow doctor` — states the baseline, reports catalog-vs-server skew, and
  names the flows under `flows/` that would not survive either.
* Push-time key translation — renamed keys are rewritten to the target's
  namespace automatically; unsupported keys are dropped **with a warning**.

## Totals

| | Processors | Controller services |
|---|---|---|
| Types on both lines | 252 | 90 |
| Types only on 2.7.2 | 40 | 33 |
| Types only on 1.24.0 | 102 | 30 |
| Types whose properties differ | 217 | 76 |
| Properties renamed between lines | 1302 | 437 |
| Properties only on 2.7.2 | 151 | 56 |
| Properties only on 1.24.0 | 241 | 23 |
| Properties with changed allowable values | 56 | 22 |
| Properties with changed required-ness | 26 | 11 |
| Properties with changed default | 13 | 7 |

Renames are **translatable** — niflow rewrites them on push. The 207 properties that exist only on 2.7.2 and the 264 that exist only on 1.24.0 are **not**: they carry a value that cannot land on the other line.

## Processors: types missing on the other line

Using one of these in a flow bound for the other line fails at push — the type simply is not installed.

**Only on 2.7.2** (40): `ConsumeKafka`, `PublishKafka`, `ConsumeKinesis`, `CopyS3Object`, `GetS3ObjectMetadata`, `GetS3ObjectTags`, `PutAzureDataExplorer`, `QueryAzureDataExplorer`, `CopyAzureBlobStorage_v12`, `ConsumeBoxEnterpriseEvents`, `ConsumeBoxEvents`, `CreateBoxFileMetadataInstance`, … (+28 more)

**Only on 1.24.0** (102): `GetHTMLElement`, `ModifyHTMLElement`, `PutHTMLElement`, `DeleteHBaseCells`, `DeleteHBaseRow`, `FetchHBaseRow`, `GetHBase`, `PutHBaseCell`, `PutHBaseJSON`, `PutHBaseRecord`, `ScanHBase`, `ConvertAvroToJSON`, … (+90 more)

## Processors: worst offenders

Ranked by unsupported properties (weighted heaviest — a value that cannot land), then contract changes, then renames, with a bonus for types in common use.

### PutS3Object

`org.apache.nifi.processors.aws.s3.PutS3Object`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`, `File Resource Service`, `Resource Transfer Source`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Expiration Time Rule`, `Owner`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `Signer Override`, `Write Permission User List`, `custom-signer-class-name`, `custom-signer-module-location`, `proxy-user-name`, … (+2 more)
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Canned ACL` ← `canned-acl`, `Encryption Service` ← `encryption-service`, `Object Tags Prefix` ← `s3-object-tags-prefix`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Remove Tag Prefix` ← `s3-object-remove-tags-prefix`, `Temporary Directory Multipart State` ← `s3-temporary-directory-multipart`, `Use Chunked Encoding` ← `use-chunked-encoding`, `Use Path Style Access` ← `use-path-style-access`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-cn-global`, `aws-global`, `aws-iso-b-global`, `aws-iso-e-global`, … (+11 more); only on 1.24.0 `attribute-defined-region`
* **Allowable values changed** for `Storage Class`: only on 2.7.2 `DEEP_ARCHIVE`, `EXPRESS_ONEZONE`, `FSX_ONTAP`, `FSX_OPENZFS`, `GLACIER`, `GLACIER_IR`, `INTELLIGENT_TIERING`, `ONEZONE_IA`, … (+6 more); only on 1.24.0 `DeepArchive`, `Glacier`, `GlacierInstantRetrieval`, `IntelligentTiering`, `OneZoneInfrequentAccess`, `Outposts`, `ReducedRedundancy`, `Snow`, … (+2 more)
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False
* **Default changed** for `Bucket`: 2.7.2 `${s3.bucket}` vs 1.24.0 `None`
* **Default changed** for `Storage Class`: 2.7.2 `STANDARD` vs 1.24.0 `Standard`

### ListS3

`org.apache.nifi.processors.aws.s3.ListS3`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `Signer Override`, `custom-signer-class-name`, `custom-signer-module-location`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Delimiter` ← `delimiter`, `Entity Tracking Initial Listing Target` ← `et-initial-listing-target`, `Entity Tracking State Cache` ← `et-state-cache`, `Entity Tracking Time Window` ← `et-time-window`, `List Type` ← `list-type`, `Listing Strategy` ← `listing-strategy`, `Maximum Object Age` ← `max-age`, `Minimum Object Age` ← `min-age`, `Prefix` ← `prefix`, … (+6 more)
* **Allowable values changed** for `Listing Strategy`: only on 2.7.2 `none`; only on 1.24.0 —
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-cn-global`, `aws-global`, `aws-iso-b-global`, `aws-iso-e-global`, … (+11 more); only on 1.24.0 —
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Entity Tracking Initial Listing Target`: 2.7.2 required=True, 1.24.0 required=False

### FetchS3Object

`org.apache.nifi.processors.aws.s3.FetchS3Object`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `Signer Override`, `custom-signer-class-name`, `custom-signer-module-location`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Encryption Service` ← `encryption-service`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Range Length` ← `range-length`, `Range Start` ← `range-start`, `Requester Pays` ← `requester-pays`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-cn-global`, `aws-global`, `aws-iso-b-global`, `aws-iso-e-global`, … (+11 more); only on 1.24.0 `attribute-defined-region`
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False
* **Default changed** for `Bucket`: 2.7.2 `${s3.bucket}` vs 1.24.0 `None`

### InvokeHTTP

`org.apache.nifi.processors.standard.InvokeHTTP`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `OAuth2 Access Token Refresh Strategy`, `Response Header Request Attributes Prefix`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Proxy Host`, `Proxy Port`, `Proxy Type`, `invokehttp-proxy-password`, `invokehttp-proxy-user`
* **Renamed** (translated automatically): `HTTP URL` ← `Remote URL`, `HTTP/2 Disabled` ← `disable-http2`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Request Body Enabled` ← `send-message-body`, `Request Chunked Transfer-Encoding Enabled` ← `Use Chunked Encoding`, `Request Content-Encoding` ← `Content-Encoding`, `Request Content-Type` ← `Content-Type`, `Request Date Header Enabled` ← `Include Date Header`, `Request Digest Authentication Enabled` ← `Digest Authentication`, `Request Failure Penalization Enabled` ← `Penalize on "No Retry"`, … (+20 more)
* **Required-ness changed** for `Request Digest Authentication Enabled`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Response Body Attribute Size`: 2.7.2 required=True, 1.24.0 required=False

### PutDatabaseRecord

`org.apache.nifi.processors.standard.PutDatabaseRecord`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Binary String Format`, `Column Name Translation Pattern`, `Column Name Translation Strategy`, `Database Dialect Service`, `Database Session AutoCommit`, `Delete Keys`
* **Renamed** (translated automatically): `Allow Multiple SQL Statements` ← `put-db-record-allow-multiple-statements`, `Database Connection Pooling Service` ← `put-db-record-dcbp-service`, `Database Name` ← `put-db-record-catalog-name`, `Database Type` ← `db-type`, `Field Containing SQL` ← `put-db-record-field-containing-sql`, `Max Wait Time` ← `put-db-record-query-timeout`, `Maximum Batch Size` ← `put-db-record-max-batch-size`, `Quote Column Identifiers` ← `put-db-record-quoted-identifiers`, `Quote Table Identifiers` ← `put-db-record-quoted-table-identifiers`, `Record Reader` ← `put-db-record-record-reader`, … (+9 more)
* **Allowable values changed** for `Database Type`: only on 2.7.2 `Database Dialect Service`; only on 1.24.0 —
* **Required-ness changed** for `Database Type`: 2.7.2 required=True, 1.24.0 required=False

### DeleteS3Object

`org.apache.nifi.processors.aws.s3.DeleteS3Object`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `FullControl User List`, `Owner`, `Proxy Host`, `Proxy Host Port`, `Read ACL User List`, `Read Permission User List`, `Secret Key`, `Signer Override`, `Write ACL User List`, `Write Permission User List`, … (+4 more)
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Proxy Configuration Service` ← `proxy-configuration-service`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-cn-global`, `aws-global`, `aws-iso-b-global`, `aws-iso-e-global`, … (+11 more); only on 1.24.0 `attribute-defined-region`
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False
* **Default changed** for `Bucket`: 2.7.2 `${s3.bucket}` vs 1.24.0 `None`

### ListSFTP

`org.apache.nifi.processors.standard.ListSFTP`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Algorithm Negotiation`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Distributed Cache Service`, `Http Proxy Password`, `Http Proxy Username`, `Proxy Host`, `Proxy Port`, `Proxy Type`
* **Renamed** (translated automatically): `Entity Tracking Initial Listing Target` ← `et-initial-listing-target`, `Entity Tracking State Cache` ← `et-state-cache`, `Entity Tracking Time Window` ← `et-time-window`, `Follow Symbolic Links` ← `follow-symlink`, `Listing Strategy` ← `listing-strategy`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Record Writer` ← `record-writer`, `Target System Timestamp Precision` ← `target-system-timestamp-precision`

### FetchSFTP

`org.apache.nifi.processors.standard.FetchSFTP`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Algorithm Negotiation`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Http Proxy Password`, `Http Proxy Username`, `Proxy Host`, `Proxy Port`, `Proxy Type`
* **Renamed** (translated automatically): `Log Level When File Not Found` ← `fetchfiletransfer-notfound-loglevel`, `Proxy Configuration Service` ← `proxy-configuration-service`

### GetSFTP

`org.apache.nifi.processors.standard.GetSFTP`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Algorithm Negotiation`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Http Proxy Password`, `Http Proxy Username`, `Proxy Host`, `Proxy Port`, `Proxy Type`
* **Renamed** (translated automatically): `Follow Symbolic Links` ← `follow-symlink`, `Proxy Configuration Service` ← `proxy-configuration-service`

### PutSFTP

`org.apache.nifi.processors.standard.PutSFTP`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Algorithm Negotiation`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Http Proxy Password`, `Http Proxy Username`, `Proxy Host`, `Proxy Port`, `Proxy Type`
* **Renamed** (translated automatically): `Proxy Configuration Service` ← `proxy-configuration-service`

### PutElasticsearchRecord

`org.apache.nifi.processors.elasticsearch.PutElasticsearchRecord`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Max JSON Field String Length`
* **Renamed** (translated automatically): `Batch Size` ← `put-es-record-batch-size`, `Client Service` ← `el-rest-client-service`, `Date Format` ← `put-es-record-at-timestamp-date-format`, `Dynamic Templates Record Path` ← `put-es-record-dynamic-templates-path`, `Group Results by Bulk Error Type` ← `put-es-record-bulk-error-groups`, `ID Record Path` ← `put-es-record-id-path`, `Index` ← `el-rest-fetch-index`, `Index Operation` ← `put-es-record-index-op`, `Index Operation Record Path` ← `put-es-record-index-op-path`, `Index Record Path` ← `put-es-record-index-record-path`, … (+15 more)
* **Allowable values changed** for `Retain Record Path ID Field`: only on 2.7.2 —; only on 1.24.0 `false`, `true`
* **Allowable values changed** for `Retain Record Timestamp`: only on 2.7.2 —; only on 1.24.0 `false`, `true`
* **Required-ness changed** for `Result Record Writer`: 2.7.2 required=True, 1.24.0 required=False

### ValidateRecord

`org.apache.nifi.processors.standard.ValidateRecord`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Branch`, `Schema Version`
* **Renamed** (translated automatically): `Allow Extra Fields` ← `allow-extra-fields`, `Force Types From Reader Schema` ← `coerce-types`, `Maximum Validation Details Length` ← `maximum-validation-details-length`, `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`, `Record Writer for Invalid Records` ← `invalid-record-writer`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, … (+2 more)
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `schema-name`; only on 1.24.0 `schema-name-property`

### TagS3Object

`org.apache.nifi.processors.aws.s3.TagS3Object`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `Signer Override`, `custom-signer-class-name`, `custom-signer-module-location`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Append Tag` ← `append-tag`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Tag Key` ← `tag-key`, `Tag Value` ← `tag-value`, `Version` ← `version`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-cn-global`, `aws-global`, `aws-iso-b-global`, `aws-iso-e-global`, … (+11 more); only on 1.24.0 `attribute-defined-region`
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False
* **Default changed** for `Bucket`: 2.7.2 `${s3.bucket}` vs 1.24.0 `None`

### PutSNS

`org.apache.nifi.processors.aws.sns.PutSNS`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`, `Endpoint Override URL`, `Proxy Configuration Service`, `SSL Context Service`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Amazon Resource Name` ← `Amazon Resource Name (ARN)`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-iso-e-global`, `aws-iso-f-global`, `ca-west-1`, `eu-isoe-west-1`, … (+6 more); only on 1.24.0 —
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False

### ExecuteSQL

`org.apache.nifi.processors.standard.ExecuteSQL`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Content Output Strategy`
* **Renamed** (translated automatically): `Compression Format` ← `compression-format`, `Default Decimal Precision` ← `dbf-default-precision`, `Default Decimal Scale` ← `dbf-default-scale`, `Fetch Size` ← `esql-fetch-size`, `Max Rows Per Flow File` ← `esql-max-rows`, `Normalize Table and Column Names` ← `dbf-normalize`, `Output Batch Size` ← `esql-output-batch-size`, `SQL Post-Query` ← `sql-post-query`, `SQL Pre-Query` ← `sql-pre-query`, `SQL Query` ← `SQL select query`, … (+2 more)

### LookupRecord

`org.apache.nifi.processors.standard.LookupRecord`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Root Record Path`
* **Renamed** (translated automatically): `Cache Size` ← `record-path-lookup-miss-result-cache-size`, `Lookup Service` ← `lookup-service`, `Record Reader` ← `record-reader`, `Record Result Contents` ← `result-contents`, `Record Update Strategy` ← `record-update-strategy`, `Record Writer` ← `record-writer`, `Result RecordPath` ← `result-record-path`, `Routing Strategy` ← `routing-strategy`

### QueryRecord

`org.apache.nifi.processors.standard.QueryRecord`

* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `cache-schema`
* **Renamed** (translated automatically): `Default Decimal Precision` ← `dbf-default-precision`, `Default Decimal Scale` ← `dbf-default-scale`, `Include Zero Record FlowFiles` ← `include-zero-record-flowfiles`, `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`

### PutSQS

`org.apache.nifi.processors.aws.sqs.PutSQS`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`, `Proxy Configuration Service`, `SSL Context Service`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`, `Deduplication Message ID` ← `deduplication-message-id`, `Message Group ID` ← `message-group-id`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-iso-e-global`, `aws-iso-f-global`, `ca-west-1`, `eu-isoe-west-1`, … (+6 more); only on 1.24.0 —
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False

### ExecuteSQLRecord

`org.apache.nifi.processors.standard.ExecuteSQLRecord`

* **Renamed** (translated automatically): `Default Decimal Precision` ← `dbf-default-precision`, `Default Decimal Scale` ← `dbf-default-scale`, `Fetch Size` ← `esql-fetch-size`, `Max Rows Per Flow File` ← `esql-max-rows`, `Normalize Table/Column Names` ← `esqlrecord-normalize`, `Output Batch Size` ← `esql-output-batch-size`, `Record Writer` ← `esqlrecord-record-writer`, `SQL Post-Query` ← `sql-post-query`, `SQL Pre-Query` ← `sql-pre-query`, `SQL Query` ← `SQL select query`, … (+2 more)

### DeleteSQS

`org.apache.nifi.processors.aws.sqs.DeleteSQS`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Custom Region`, `Proxy Configuration Service`, `SSL Context Service`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `Access Key`, `Credentials File`, `Proxy Host`, `Proxy Host Port`, `Secret Key`, `proxy-user-name`, `proxy-user-password`
* **Renamed** (translated automatically): `AWS Credentials Provider Service` ← `AWS Credentials Provider service`
* **Allowable values changed** for `Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `aws-iso-e-global`, `aws-iso-f-global`, `ca-west-1`, `eu-isoe-west-1`, … (+6 more); only on 1.24.0 —
* **Required-ness changed** for `AWS Credentials Provider Service`: 2.7.2 required=True, 1.24.0 required=False

## Processors: other everyday types

These score low — mostly renames, which niflow translates for you —
but they are the ones you actually use, so here they are in full.

### ConvertRecord

`org.apache.nifi.processors.standard.ConvertRecord`

* **Renamed** (translated automatically): `Include Zero Record FlowFiles` ← `include-zero-record-flowfiles`, `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`

### FetchFile

`org.apache.nifi.processors.standard.FetchFile`

* **Renamed** (translated automatically): `File not Found Log Level` ← `Log level when file not found`, `Permission Denied Log Level` ← `Log level when permission denied`

### GenerateFlowFile

`org.apache.nifi.processors.standard.GenerateFlowFile`

* **Renamed** (translated automatically): `Character Set` ← `character-set`, `Custom Text` ← `generate-ff-custom-text`, `Mime Type` ← `mime-type`

### GetFile

`org.apache.nifi.processors.standard.GetFile`

* **Default changed** for `Polling Interval`: 2.7.2 `30 sec` vs 1.24.0 `0 sec`

### ListFile

`org.apache.nifi.processors.standard.ListFile`

* **Renamed** (translated automatically): `Entity Tracking Initial Listing Target` ← `et-initial-listing-target`, `Entity Tracking Node Identifier` ← `et-node-identifier`, `Entity Tracking State Cache` ← `et-state-cache`, `Entity Tracking Time Window` ← `et-time-window`, `Listing Strategy` ← `listing-strategy`, `Max Directory Listing Time` ← `max-listing-time`, `Max Disk Operation Time` ← `max-operation-time`, `Maximum Number of Files to Track` ← `max-performance-metrics`, `Record Writer` ← `record-writer`, `Target System Timestamp Precision` ← `target-system-timestamp-precision`, … (+1 more)

### LogAttribute

`org.apache.nifi.processors.standard.LogAttribute`

* **Renamed** (translated automatically): `Attributes to Ignore Regular Expression` ← `attributes-to-ignore-regex`, `Attributes to Log Regular Expression` ← `attributes-to-log-regex`, `Character Set` ← `character-set`, `Log Prefix` ← `Log prefix`

### MergeRecord

`org.apache.nifi.processors.standard.MergeRecord`

* **Renamed** (translated automatically): `Correlation Attribute Name` ← `correlation-attribute-name`, `Max Bin Age` ← `max-bin-age`, `Maximum Bin Size` ← `max-bin-size`, `Maximum Number of Bins` ← `max.bin.count`, `Maximum Number of Records` ← `max-records`, `Merge Strategy` ← `merge-strategy`, `Minimum Bin Size` ← `min-bin-size`, `Minimum Number of Records` ← `min-records`, `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`

### PartitionRecord

`org.apache.nifi.processors.standard.PartitionRecord`

* **Renamed** (translated automatically): `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`

### ReplaceText

`org.apache.nifi.processors.standard.ReplaceText`

* **Renamed** (translated automatically): `Search Value` ← `Regular Expression`

### UpdateRecord

`org.apache.nifi.processors.standard.UpdateRecord`

* **Renamed** (translated automatically): `Record Reader` ← `record-reader`, `Record Writer` ← `record-writer`, `Replacement Value Strategy` ← `replacement-value-strategy`

## Processors: complete index

Every type whose properties differ. `renamed` is handled for you; `only 2.7.2` and `only 1.24.0` are the counts that can bite. Full detail for every one of them is in `niflow/version_map.py`.

| Type | renamed | only 2.7.2 | only 1.24.0 | allowable | required |
|---|---|---|---|---|---|
| `AttributeRollingWindow` | 3 | 0 | 0 | 0 | 0 |
| `AttributesToCSV` | 6 | 0 | 0 | 0 | 0 |
| `AttributesToJSON` | 1 | 1 | 0 | 0 | 0 |
| `CalculateRecordStats` | 2 | 0 | 0 | 0 | 0 |
| `CaptureChangeMySQL` | 19 | 0 | 2 | 0 | 0 |
| `ConnectWebSocket` | 2 | 0 | 0 | 0 | 0 |
| `ConsumeAMQP` | 9 | 2 | 1 | 0 | 0 |
| `ConsumeAzureEventHub` | 16 | 5 | 2 | 0 | 0 |
| `ConsumeElasticsearch` | 22 | 1 | 0 | 0 | 0 |
| `ConsumeGCPubSub` | 5 | 5 | 0 | 0 | 0 |
| `ConsumeIMAP` | 10 | 0 | 0 | 0 | 0 |
| `ConsumeJMS` | 16 | 1 | 1 | 0 | 0 |
| `ConsumeKinesisStream` | 16 | 3 | 0 | 1 | 0 |
| `ConsumeMQTT` | 8 | 0 | 0 | 0 | 0 |
| `ConsumePOP3` | 10 | 0 | 0 | 0 | 0 |
| `ConsumeTwitter` | 17 | 0 | 0 | 0 | 0 |
| `ConsumeWindowsEventLog` | 5 | 0 | 0 | 0 | 0 |
| `ControlRate` | 0 | 1 | 0 | 0 | 0 |
| `ConvertRecord` | 3 | 0 | 0 | 0 | 0 |
| `CountText` | 7 | 0 | 0 | 0 | 0 |
| `CryptographicHashContent` | 2 | 0 | 0 | 0 | 0 |
| `DebugFlow` | 3 | 2 | 0 | 0 | 0 |
| `DecryptContentPGP` | 3 | 0 | 0 | 0 | 0 |
| `DeduplicateRecord` | 11 | 0 | 0 | 0 | 0 |
| `DeleteAzureBlobStorage_v12` | 5 | 0 | 0 | 0 | 0 |
| `DeleteAzureDataLakeStorage` | 6 | 0 | 0 | 0 | 0 |
| `DeleteByQueryElasticsearch` | 7 | 1 | 0 | 0 | 0 |
| `DeleteDynamoDB` | 3 | 2 | 7 | 1 | 1 |
| `DeleteGCSObject` | 7 | 0 | 4 | 0 | 0 |
| `DeleteGridFS` | 6 | 0 | 0 | 0 | 0 |
| `DeleteMongo` | 3 | 0 | 4 | 0 | 0 |
| `DeleteS3Object` | 2 | 1 | 16 | 1 | 1 |
| `DeleteSQS` | 1 | 3 | 7 | 1 | 1 |
| `DistributeLoad` | 0 | 0 | 0 | 1 | 0 |
| `EncodeContent` | 0 | 2 | 0 | 0 | 0 |
| `EncryptContentPGP` | 5 | 0 | 1 | 1 | 0 |
| `EnforceOrder` | 7 | 0 | 0 | 0 | 0 |
| `EvaluateXPath` | 1 | 0 | 0 | 0 | 0 |
| `EvaluateXQuery` | 1 | 0 | 0 | 0 | 0 |
| `ExecuteGroovyScript` | 4 | 0 | 0 | 0 | 0 |
| `ExecuteProcess` | 1 | 0 | 0 | 0 | 0 |
| `ExecuteSQL` | 12 | 1 | 0 | 0 | 0 |
| `ExecuteSQLRecord` | 12 | 0 | 0 | 0 | 0 |
| `ExecuteScript` | 0 | 0 | 0 | 1 | 0 |
| `ExecuteStreamCommand` | 1 | 0 | 0 | 0 | 0 |
| `ExtractEmailHeaders` | 2 | 0 | 0 | 0 | 0 |
| `ExtractGrok` | 2 | 0 | 0 | 0 | 0 |
| `ExtractHL7Attributes` | 4 | 0 | 0 | 0 | 0 |
| `ExtractRecordSchema` | 2 | 0 | 0 | 0 | 0 |
| `ExtractText` | 2 | 0 | 0 | 0 | 0 |
| `FetchAzureBlobStorage_v12` | 6 | 0 | 0 | 0 | 0 |
| `FetchAzureDataLakeStorage` | 8 | 0 | 0 | 0 | 0 |
| `FetchBoxFile` | 2 | 0 | 0 | 0 | 0 |
| `FetchDropbox` | 3 | 0 | 0 | 0 | 0 |
| `FetchFTP` | 3 | 0 | 5 | 0 | 0 |
| `FetchFile` | 2 | 0 | 0 | 0 | 0 |
| `FetchGCSObject` | 10 | 0 | 4 | 0 | 0 |
| `FetchGoogleDrive` | 3 | 2 | 0 | 2 | 0 |
| `FetchGridFS` | 7 | 0 | 0 | 0 | 0 |
| `FetchS3Object` | 6 | 1 | 10 | 1 | 1 |
| `FetchSFTP` | 2 | 1 | 5 | 0 | 0 |
| `FetchSmb` | 2 | 3 | 0 | 0 | 0 |
| `FlattenJson` | 6 | 0 | 0 | 0 | 0 |
| `ForkRecord` | 4 | 0 | 0 | 0 | 0 |
| `GenerateFlowFile` | 3 | 0 | 0 | 0 | 0 |
| `GenerateRecord` | 5 | 0 | 0 | 0 | 0 |
| `GenerateTableFetch` | 6 | 1 | 0 | 1 | 0 |
| `GeoEnrichIP` | 1 | 0 | 0 | 0 | 0 |
| `GeoEnrichIPRecord` | 11 | 0 | 0 | 0 | 0 |
| `GeohashRecord` | 9 | 0 | 0 | 0 | 0 |
| `GetAsanaObject` | 8 | 0 | 0 | 0 | 0 |
| `GetAwsPollyJobStatus` | 4 | 1 | 0 | 1 | 0 |
| `GetAwsTextractJobStatus` | 5 | 1 | 0 | 2 | 0 |
| `GetAwsTranscribeJobStatus` | 4 | 1 | 0 | 1 | 0 |
| `GetAwsTranslateJobStatus` | 4 | 1 | 0 | 1 | 0 |
| `GetAzureEventHub` | 6 | 2 | 2 | 0 | 0 |
| `GetAzureQueueStorage_v12` | 2 | 0 | 0 | 0 | 0 |
| `GetDynamoDB` | 4 | 2 | 7 | 1 | 1 |
| `GetElasticsearch` | 6 | 0 | 0 | 0 | 0 |
| `GetFTP` | 3 | 0 | 5 | 0 | 0 |
| `GetFile` | 0 | 0 | 0 | 0 | 0 |
| `GetGcpVisionAnnotateFilesOperationStatus` | 2 | 0 | 0 | 0 | 0 |
| `GetGcpVisionAnnotateImagesOperationStatus` | 2 | 0 | 0 | 0 | 0 |
| `GetHubSpot` | 7 | 0 | 0 | 0 | 0 |
| `GetMongo` | 8 | 0 | 3 | 0 | 0 |
| `GetMongoRecord` | 4 | 0 | 0 | 0 | 0 |
| `GetSFTP` | 2 | 1 | 5 | 0 | 0 |
| `GetSNMP` | 15 | 0 | 0 | 2 | 1 |
| `GetSQS` | 1 | 3 | 7 | 1 | 1 |
| `GetShopify` | 9 | 0 | 0 | 0 | 0 |
| `GetSmbFile` | 2 | 2 | 0 | 0 | 0 |
| `GetSplunk` | 0 | 1 | 0 | 1 | 0 |
| `GetWorkdayReport` | 2 | 2 | 0 | 0 | 0 |
| `GetZendesk` | 8 | 0 | 0 | 0 | 0 |
| `HandleHttpRequest` | 4 | 1 | 0 | 0 | 0 |
| `HandleHttpResponse` | 1 | 0 | 0 | 0 | 0 |
| `ISPEnrichIP` | 1 | 0 | 0 | 0 | 0 |
| `IdentifyMimeType` | 1 | 2 | 2 | 0 | 0 |
| `InvokeHTTP` | 30 | 2 | 5 | 0 | 2 |
| `InvokeScriptedProcessor` | 0 | 0 | 0 | 1 | 0 |
| `JSLTTransformJSON` | 5 | 0 | 0 | 0 | 0 |
| `JoinEnrichment` | 3 | 0 | 0 | 0 | 0 |
| `JsonQueryElasticsearch` | 17 | 1 | 0 | 0 | 0 |
| `ListAzureBlobStorage_v12` | 9 | 0 | 0 | 0 | 0 |
| `ListAzureDataLakeStorage` | 13 | 0 | 0 | 0 | 0 |
| `ListBoxFile` | 9 | 0 | 0 | 0 | 0 |
| `ListDatabaseTables` | 8 | 0 | 0 | 0 | 0 |
| `ListDropbox` | 10 | 0 | 0 | 0 | 0 |
| `ListFTP` | 9 | 0 | 6 | 0 | 0 |
| `ListFile` | 11 | 0 | 0 | 0 | 0 |
| `ListGCSBucket` | 12 | 0 | 4 | 0 | 0 |
| `ListGoogleDrive` | 10 | 2 | 0 | 0 | 0 |
| `ListS3` | 16 | 1 | 10 | 2 | 2 |
| `ListSFTP` | 8 | 1 | 6 | 0 | 0 |
| `ListSmb` | 13 | 4 | 0 | 0 | 0 |
| `ListenFTP` | 5 | 0 | 0 | 0 | 0 |
| `ListenHTTP` | 10 | 1 | 1 | 0 | 0 |
| `ListenSyslog` | 0 | 3 | 3 | 0 | 0 |
| `ListenTCP` | 4 | 0 | 1 | 0 | 1 |
| `ListenTrapSNMP` | 8 | 0 | 0 | 0 | 0 |
| `ListenUDP` | 1 | 0 | 0 | 0 | 0 |
| `ListenUDPRecord` | 6 | 0 | 0 | 0 | 0 |
| `ListenWebSocket` | 2 | 0 | 0 | 0 | 0 |
| `LogAttribute` | 4 | 0 | 0 | 0 | 0 |
| `LogMessage` | 3 | 0 | 0 | 0 | 0 |
| `LookupAttribute` | 2 | 0 | 0 | 0 | 0 |
| `LookupRecord` | 8 | 1 | 0 | 0 | 0 |
| `MergeContent` | 5 | 2 | 0 | 0 | 0 |
| `MergeRecord` | 10 | 0 | 0 | 0 | 0 |
| `MonitorActivity` | 0 | 2 | 0 | 0 | 1 |
| `MoveAzureDataLakeStorage` | 8 | 0 | 0 | 0 | 0 |
| `Notify` | 6 | 0 | 0 | 0 | 0 |
| `PackageFlowFile` | 1 | 1 | 0 | 0 | 0 |
| `PaginatedJsonQueryElasticsearch` | 19 | 1 | 0 | 0 | 0 |
| `ParseEvtx` | 1 | 0 | 0 | 0 | 0 |
| `ParseNetflowv5` | 1 | 0 | 0 | 0 | 0 |
| `ParseSyslog5424` | 2 | 0 | 0 | 0 | 0 |
| `PartitionRecord` | 2 | 0 | 0 | 0 | 0 |
| `PublishAMQP` | 4 | 2 | 1 | 0 | 0 |
| `PublishGCPubSub` | 7 | 0 | 0 | 0 | 0 |
| `PublishJMS` | 16 | 1 | 1 | 0 | 0 |
| `PublishMQTT` | 7 | 0 | 0 | 0 | 0 |
| `PutAzureBlobStorage_v12` | 6 | 0 | 0 | 0 | 0 |
| `PutAzureCosmosDBRecord` | 10 | 0 | 0 | 0 | 0 |
| `PutAzureDataLakeStorage` | 7 | 1 | 0 | 0 | 0 |
| `PutAzureEventHub` | 4 | 2 | 1 | 0 | 0 |
| `PutAzureQueueStorage_v12` | 2 | 0 | 0 | 0 | 0 |
| `PutBigQuery` | 9 | 1 | 0 | 0 | 0 |
| `PutBoxFile` | 7 | 0 | 0 | 0 | 0 |
| `PutCloudWatchMetric` | 6 | 2 | 7 | 1 | 1 |
| `PutDatabaseRecord` | 19 | 6 | 0 | 1 | 1 |
| `PutDistributedMapCache` | 2 | 0 | 0 | 0 | 0 |
| `PutDropbox` | 7 | 0 | 0 | 0 | 0 |
| `PutDynamoDB` | 5 | 2 | 7 | 1 | 1 |
| `PutDynamoDBRecord` | 8 | 2 | 0 | 1 | 0 |
| `PutElasticsearchJson` | 13 | 1 | 1 | 1 | 0 |
| `PutElasticsearchRecord` | 25 | 1 | 0 | 2 | 1 |
| `PutEmail` | 6 | 1 | 0 | 0 | 0 |
| `PutFTP` | 2 | 0 | 5 | 0 | 0 |
| `PutGCSObject` | 13 | 2 | 5 | 0 | 0 |
| `PutGoogleDrive` | 7 | 2 | 0 | 0 | 0 |
| `PutGridFS` | 8 | 0 | 0 | 0 | 0 |
| `PutKinesisFirehose` | 3 | 1 | 7 | 1 | 1 |
| `PutKinesisStream` | 6 | 1 | 7 | 1 | 1 |
| `PutLambda` | 3 | 2 | 7 | 1 | 1 |
| `PutMongo` | 3 | 1 | 4 | 0 | 0 |
| `PutMongoRecord` | 7 | 0 | 4 | 0 | 0 |
| `PutRecord` | 3 | 0 | 0 | 0 | 0 |
| `PutS3Object` | 9 | 3 | 14 | 2 | 1 |
| `PutSFTP` | 1 | 1 | 5 | 0 | 0 |
| `PutSNS` | 2 | 4 | 7 | 1 | 1 |
| `PutSQL` | 3 | 0 | 0 | 0 | 0 |
| `PutSQS` | 3 | 3 | 7 | 1 | 1 |
| `PutSalesforceObject` | 5 | 0 | 0 | 0 | 0 |
| `PutSmbFile` | 2 | 2 | 0 | 0 | 0 |
| `PutSplunkHTTP` | 9 | 0 | 0 | 0 | 0 |
| `PutWebSocket` | 4 | 0 | 0 | 0 | 0 |
| `QueryAirtableTable` | 10 | 0 | 0 | 0 | 0 |
| `QueryDatabaseTable` | 12 | 2 | 0 | 1 | 0 |
| `QueryDatabaseTableRecord` | 12 | 2 | 0 | 1 | 0 |
| `QueryRecord` | 5 | 0 | 1 | 0 | 0 |
| `QuerySalesforceObject` | 14 | 1 | 0 | 0 | 0 |
| `QuerySplunkIndexingStatus` | 5 | 0 | 0 | 0 | 0 |
| `RemoveRecordField` | 2 | 0 | 0 | 0 | 0 |
| `ReplaceText` | 1 | 0 | 0 | 0 | 0 |
| `RetryFlowFile` | 5 | 0 | 0 | 0 | 0 |
| `RunMongoAggregation` | 8 | 0 | 3 | 0 | 0 |
| `SampleRecord` | 8 | 0 | 0 | 0 | 0 |
| `ScriptedFilterRecord` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedPartitionRecord` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedTransformRecord` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedValidateRecord` | 0 | 0 | 0 | 1 | 0 |
| `SearchElasticsearch` | 19 | 2 | 0 | 0 | 0 |
| `SendTrapSNMP` | 17 | 0 | 0 | 2 | 0 |
| `SetSNMP` | 12 | 0 | 0 | 2 | 0 |
| `SignContentPGP` | 5 | 0 | 1 | 0 | 0 |
| `StartAwsPollyJob` | 3 | 1 | 0 | 1 | 0 |
| `StartAwsTextractJob` | 4 | 1 | 0 | 2 | 0 |
| `StartAwsTranscribeJob` | 3 | 1 | 0 | 1 | 0 |
| `StartAwsTranslateJob` | 3 | 1 | 0 | 1 | 0 |
| `StartGcpVisionAnnotateFilesOperation` | 4 | 0 | 0 | 0 | 0 |
| `StartGcpVisionAnnotateImagesOperation` | 4 | 0 | 0 | 0 | 0 |
| `TagS3Object` | 6 | 1 | 10 | 1 | 1 |
| `TailFile` | 9 | 0 | 0 | 0 | 0 |
| `TransformXml` | 7 | 0 | 0 | 0 | 0 |
| `UnpackContent` | 1 | 1 | 0 | 0 | 0 |
| `UpdateAttribute` | 1 | 0 | 0 | 0 | 0 |
| `UpdateByQueryElasticsearch` | 8 | 1 | 0 | 0 | 0 |
| `UpdateCounter` | 2 | 0 | 0 | 0 | 0 |
| `UpdateDatabaseTable` | 14 | 3 | 0 | 1 | 1 |
| `UpdateRecord` | 3 | 0 | 0 | 0 | 0 |
| `ValidateCsv` | 6 | 3 | 0 | 0 | 1 |
| `ValidateJson` | 1 | 4 | 0 | 0 | 0 |
| `ValidateRecord` | 12 | 2 | 0 | 1 | 0 |
| `VerifyContentMAC` | 5 | 0 | 0 | 0 | 0 |
| `VerifyContentPGP` | 1 | 0 | 0 | 0 | 0 |
| `Wait` | 10 | 0 | 0 | 0 | 0 |

## Controller services: types missing on the other line

Using one of these in a flow bound for the other line fails at push — the type simply is not installed.

**Only on 2.7.2** (33): `ApicurioSchemaRegistry`, `AmazonGlueEncodedSchemaReferenceReader`, `DeveloperBoxClientService`, `ConfluentEncodedSchemaReferenceReader`, `ConfluentEncodedSchemaReferenceWriter`, `ConfluentProtobufMessageNameResolver`, `StandardDatabaseDialectService`, `DatabaseTableSchemaRegistry`, `MapCacheClientService`, `SetCacheClientService`, `SetCacheServer`, `MapCacheServer`, … (+21 more)

**Only on 1.24.0** (30): `CassandraDistributedMapCache`, `KuduLookupService`, `CouchbaseClusterService`, `CouchbaseKeyValueLookupService`, `CouchbaseMapCacheClient`, `CouchbaseRecordLookupService`, `HadoopDBCPConnectionPool`, `DistributedMapCacheClientService`, `DistributedSetCacheClientService`, `DistributedSetCacheServer`, `DistributedMapCacheServer`, `HBase_2_ClientMapCacheService`, … (+18 more)

## Controller services: worst offenders

Ranked by unsupported properties (weighted heaviest — a value that cannot land), then contract changes, then renames, with a bonus for types in common use.

### DBCPConnectionPool

`org.apache.nifi.dbcp.DBCPConnectionPool`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Database Password Provider`, `Password Source`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `kerberos-credentials-service`, `kerberos-password`, `kerberos-principal`
* **Renamed** (translated automatically): `Database Driver Locations` ← `database-driver-locations`, `Kerberos User Service` ← `kerberos-user-service`, `Maximum Connection Lifetime` ← `dbcp-max-conn-lifetime`, `Maximum Idle Connections` ← `dbcp-max-idle-conns`, `Minimum Evictable Idle Time` ← `dbcp-min-evictable-idle-time`, `Minimum Idle Connections` ← `dbcp-min-idle-conns`, `Soft Minimum Evictable Idle Time` ← `dbcp-soft-min-evictable-idle-time`, `Time Between Eviction Runs` ← `dbcp-time-between-eviction-runs`, `Validation Query` ← `Validation-query`

### JsonRecordSetWriter

`org.apache.nifi.json.JsonRecordSetWriter`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Allow Scientific Notation`, `Schema Reference Reader`, `Schema Reference Writer`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `schema-protocol-version`
* **Renamed** (translated automatically): `Compression Format` ← `compression-format`, `Compression Level` ← `compression-level`, `Output Grouping` ← `output-grouping`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Cache` ← `schema-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`, … (+1 more)
* **Allowable values changed** for `Schema Write Strategy`: only on 2.7.2 `schema-reference-writer`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### CSVRecordSetWriter

`org.apache.nifi.csv.CSVRecordSetWriter`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `CSV Writer`, `Schema Reference Reader`, `Schema Reference Writer`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `schema-protocol-version`
* **Renamed** (translated automatically): `Character Set` ← `csvutils-character-set`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Cache` ← `schema-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`
* **Allowable values changed** for `Schema Write Strategy`: only on 2.7.2 `schema-reference-writer`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### AvroRecordSetWriter

`org.apache.nifi.avro.AvroRecordSetWriter`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Reference Reader`, `Schema Reference Writer`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `schema-protocol-version`
* **Renamed** (translated automatically): `Cache Size` ← `cache-size`, `Compression Format` ← `compression-format`, `Encoder Pool Size` ← `encoder-pool-size`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Cache` ← `schema-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`
* **Allowable values changed** for `Schema Write Strategy`: only on 2.7.2 `schema-reference-writer`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### CSVReader

`org.apache.nifi.csv.CSVReader`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Reference Reader`
* **Renamed** (translated automatically): `Allow Duplicate Header Names` ← `csvutils-allow-duplicate-header-names`, `CSV Parser` ← `csv-reader-csv-parser`, `Character Set` ← `csvutils-character-set`, `Ignore CSV Header Column Names` ← `ignore-csv-header`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`, … (+2 more)
* **Allowable values changed** for `CSV Parser`: only on 2.7.2 `fast-csv`; only on 1.24.0 —
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `schema-reference-reader`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### JsonTreeReader

`org.apache.nifi.json.JsonTreeReader`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Reference Reader`
* **Renamed** (translated automatically): `Schema Access Strategy` ← `schema-access-strategy`, `Schema Application Strategy` ← `schema-application-strategy`, `Schema Branch` ← `schema-branch`, `Schema Inference Cache` ← `schema-inference-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`, `Starting Field Name` ← `starting-field-name`, `Starting Field Strategy` ← `starting-field-strategy`
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `schema-reference-reader`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### AvroReader

`org.apache.nifi.avro.AvroReader`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Reference Reader`
* **Renamed** (translated automatically): `Cache Size` ← `cache-size`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `schema-reference-reader`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### GCPCredentialsControllerService

`org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Audience`, `Authentication Strategy`, `Delegation Strategy`, `Delegation User`, `STS Token Endpoint`, `Scope`, `Subject Token Provider`, `Subject Token Type`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `application-default-credentials`, `compute-engine-credentials`
* **Renamed** (translated automatically): `Proxy Configuration Service` ← `proxy-configuration-service`, `Service Account JSON` ← `service-account-json`, `Service Account JSON File` ← `service-account-json-file`
* **Required-ness changed** for `Service Account JSON`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Service Account JSON File`: 2.7.2 required=True, 1.24.0 required=False

### StandardRestrictedSSLContextService

`org.apache.nifi.ssl.StandardRestrictedSSLContextService`

* **Renamed** (translated automatically): `Key Password` ← `key-password`, `TLS Protocol` ← `SSL Protocol`

### StandardSSLContextService

`org.apache.nifi.ssl.StandardSSLContextService`

* **Renamed** (translated automatically): `Key Password` ← `key-password`, `TLS Protocol` ← `SSL Protocol`

### ElasticSearchClientServiceImpl

`org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `JWT Shared Secret`, `OAuth2 Access Token Provider`, `Run As User`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `el-cs-retry-timeout`
* **Renamed** (translated automatically): `API Key` ← `api-key`, `API Key ID` ← `api-key-id`, `Authorization Scheme` ← `authorization-scheme`, `Character Set` ← `el-cs-charset`, `Connect Timeout` ← `el-cs-connect-timeout`, `Enable Compression` ← `el-cs-enable-compression`, `HTTP Hosts` ← `el-cs-http-hosts`, `Node Selector` ← `el-cs-node-selector`, `Password` ← `el-cs-password`, `Path Prefix` ← `el-cs-path-prefix`, … (+12 more)
* **Allowable values changed** for `Authorization Scheme`: only on 2.7.2 `JWT`; only on 1.24.0 —
* **Required-ness changed** for `API Key`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `API Key ID`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Password`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Username`: 2.7.2 required=True, 1.24.0 required=False

### AWSCredentialsProviderControllerService

`org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Assume Role Proxy Configuration Service`, `OAuth2 Access Token Provider`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `assume-role-proxy-host`, `assume-role-proxy-port`, `assume-role-sts-signer-override`, `custom-signer-class-name`, `custom-signer-module-location`
* **Renamed** (translated automatically): `Access Key ID` ← `Access Key`, `Assume Role External ID` ← `assume-role-external-id`, `Assume Role SSL Context Service` ← `assume-role-ssl-context-service`, `Assume Role STS Endpoint Override` ← `assume-role-sts-endpoint`, `Assume Role STS Region` ← `assume-role-sts-region`, `Assume Role Session Time` ← `Session Time`, `Profile Name` ← `profile-name`, `Secret Access Key` ← `Secret Key`, `Use Anonymous Credentials` ← `anonymous-credentials`, `Use Default Credentials` ← `default-credentials`
* **Allowable values changed** for `Assume Role STS Region`: only on 2.7.2 `ap-east-2`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `ca-west-1`, `eu-isoe-west-1`, `eusc-de-east-1`, `mx-central-1`, … (+3 more); only on 1.24.0 —

### ExcelReader

`org.apache.nifi.excel.ExcelReader`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Input File Type`, `Password`, `Protection Type`, `Row Evaluation Strategy`, `Schema Reference Reader`
* **Renamed** (translated automatically): `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `Use Starting Row`, `schema-reference-reader`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`
* **Default changed** for `Schema Access Strategy`: 2.7.2 `Use Starting Row` vs 1.24.0 `infer-schema`

### ADLSCredentialsControllerService

`org.apache.nifi.services.azure.storage.ADLSCredentialsControllerService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Credentials Type`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `storage-use-managed-identity`
* **Renamed** (translated automatically): `Account Key` ← `storage-account-key`, `Endpoint Suffix` ← `storage-endpoint-suffix`, `Managed Identity Client ID` ← `managed-identity-client-id`, `Proxy Configuration Service` ← `proxy-configuration-service`, `SAS Token` ← `storage-sas-token`, `Service Principal Client ID` ← `service-principal-client-id`, `Service Principal Client Secret` ← `service-principal-client-secret`, `Service Principal Tenant ID` ← `service-principal-tenant-id`, `Storage Account Name` ← `storage-account-name`
* **Required-ness changed** for `Account Key`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `SAS Token`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Service Principal Client ID`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Service Principal Client Secret`: 2.7.2 required=True, 1.24.0 required=False
* **Required-ness changed** for `Service Principal Tenant ID`: 2.7.2 required=True, 1.24.0 required=False

### SiteToSiteReportingRecordSink

`org.apache.nifi.reporting.sink.SiteToSiteReportingRecordSink`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Proxy Configuration Service`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `s2s-http-proxy-hostname`, `s2s-http-proxy-password`, `s2s-http-proxy-port`, `s2s-http-proxy-username`
* **Renamed** (translated automatically): `Record Writer` ← `record-sink-record-writer`, `Transport Protocol` ← `s2s-transport-protocol`

### XMLRecordSetWriter

`org.apache.nifi.xml.XMLRecordSetWriter`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Schema Reference Reader`, `Schema Reference Writer`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `schema-protocol-version`
* **Renamed** (translated automatically): `Array Tag Name` ← `array_tag_name`, `Name of Record Tag` ← `record_tag_name`, `Name of Root Tag` ← `root_tag_name`, `Omit XML Declaration` ← `omit_xml_declaration`, `Pretty Print XML` ← `pretty_print_xml`, `Schema Access Strategy` ← `schema-access-strategy`, `Schema Branch` ← `schema-branch`, `Schema Cache` ← `schema-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, … (+4 more)
* **Allowable values changed** for `Schema Write Strategy`: only on 2.7.2 `schema-reference-writer`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### YamlTreeReader

`org.apache.nifi.yaml.YamlTreeReader`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Allow Comments`, `Max String Length`, `Schema Reference Reader`
* **Renamed** (translated automatically): `Schema Access Strategy` ← `schema-access-strategy`, `Schema Application Strategy` ← `schema-application-strategy`, `Schema Branch` ← `schema-branch`, `Schema Inference Cache` ← `schema-inference-cache`, `Schema Name` ← `schema-name`, `Schema Registry` ← `schema-registry`, `Schema Text` ← `schema-text`, `Schema Version` ← `schema-version`, `Starting Field Name` ← `starting-field-name`, `Starting Field Strategy` ← `starting-field-strategy`
* **Allowable values changed** for `Schema Access Strategy`: only on 2.7.2 `schema-reference-reader`; only on 1.24.0 `confluent-encoded`, `hwx-content-encoded-schema`, `hwx-schema-ref-attributes`

### StandardS3EncryptionService

`org.apache.nifi.processors.aws.s3.encryption.StandardS3EncryptionService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `KMS Key ID`, `Key Material`
* **Only on 1.24.0** (unreachable from a 2.7.2 flow): `key-id-or-key-material`, `kms-region`
* **Renamed** (translated automatically): `Encryption Strategy` ← `encryption-strategy`

### RestLookupService

`org.apache.nifi.lookup.RestLookupService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `Authentication Strategy`, `OAuth2 Access Token Provider`, `Response Handling Strategy`
* **Renamed** (translated automatically): `Basic Authentication Password` ← `rest-lookup-basic-auth-password`, `Basic Authentication Username` ← `rest-lookup-basic-auth-username`, `Connection Timeout` ← `rest-lookup-connection-timeout`, `Proxy Configuration Service` ← `proxy-configuration-service`, `Read Timeout` ← `rest-lookup-read-timeout`, `Record Path` ← `rest-lookup-record-path`, `Record Reader` ← `rest-lookup-record-reader`, `SSL Context Service` ← `rest-lookup-ssl-context-service`, `URL` ← `rest-lookup-url`, `Use Digest Authentication` ← `rest-lookup-digest-auth`

### JsonConfigBasedBoxClientService

`org.apache.nifi.box.controllerservices.JsonConfigBasedBoxClientService`

* **Only on 2.7.2** (dropped when pushing to 1.24.0): `App Actor`, `Connect Timeout`, `Read Timeout`
* **Renamed** (translated automatically): `Account ID` ← `box-account-id`, `App Config File` ← `app-config-file`, `App Config JSON` ← `app-config-json`, `Proxy Configuration Service` ← `proxy-configuration-service`

## Controller services: complete index

Every type whose properties differ. `renamed` is handled for you; `only 2.7.2` and `only 1.24.0` are the counts that can bite. Full detail for every one of them is in `niflow/version_map.py`.

| Type | renamed | only 2.7.2 | only 1.24.0 | allowable | required |
|---|---|---|---|---|---|
| `ADLSCredentialsControllerService` | 9 | 1 | 1 | 0 | 5 |
| `AWSCredentialsProviderControllerService` | 10 | 2 | 5 | 1 | 0 |
| `AmazonGlueSchemaRegistry` | 8 | 0 | 0 | 1 | 0 |
| `AvroReader` | 7 | 1 | 0 | 1 | 0 |
| `AvroRecordSetWriter` | 10 | 2 | 1 | 1 | 0 |
| `AvroSchemaRegistry` | 1 | 0 | 0 | 0 | 0 |
| `AzureCosmosDBClientService` | 3 | 0 | 0 | 0 | 0 |
| `AzureEventHubRecordSink` | 1 | 0 | 0 | 0 | 0 |
| `AzureStorageCredentialsControllerService_v12` | 10 | 0 | 0 | 0 | 0 |
| `CEFReader` | 12 | 1 | 0 | 1 | 0 |
| `CSVReader` | 12 | 1 | 0 | 2 | 0 |
| `CSVRecordLookupService` | 3 | 0 | 0 | 0 | 0 |
| `CSVRecordSetWriter` | 8 | 3 | 1 | 1 | 0 |
| `ConfluentSchemaRegistry` | 8 | 0 | 0 | 0 | 0 |
| `DBCPConnectionPool` | 9 | 2 | 3 | 0 | 0 |
| `DatabaseRecordLookupService` | 8 | 0 | 0 | 0 | 0 |
| `DatabaseRecordSink` | 10 | 0 | 0 | 0 | 0 |
| `DistributedMapCacheLookupService` | 2 | 0 | 0 | 0 | 0 |
| `ElasticSearchClientServiceImpl` | 22 | 3 | 1 | 1 | 4 |
| `ElasticSearchLookupService` | 9 | 0 | 0 | 0 | 0 |
| `ElasticSearchStringLookupService` | 3 | 0 | 0 | 0 | 0 |
| `EmailRecordSink` | 14 | 0 | 0 | 0 | 0 |
| `EmbeddedHazelcastCacheManager` | 4 | 0 | 0 | 0 | 0 |
| `ExcelReader` | 6 | 5 | 0 | 1 | 0 |
| `ExternalHazelcastCacheManager` | 6 | 0 | 0 | 0 | 0 |
| `GCPCredentialsControllerService` | 3 | 8 | 2 | 0 | 2 |
| `GrokReader` | 9 | 1 | 0 | 1 | 0 |
| `HazelcastMapCacheClient` | 3 | 0 | 0 | 0 | 0 |
| `HikariCPConnectionPool` | 11 | 0 | 0 | 0 | 0 |
| `IPLookupService` | 6 | 0 | 0 | 0 | 0 |
| `JMSConnectionFactoryProvider` | 4 | 0 | 0 | 0 | 0 |
| `JettyWebSocketClient` | 14 | 1 | 0 | 0 | 0 |
| `JettyWebSocketServer` | 11 | 1 | 0 | 0 | 0 |
| `JndiJmsConnectionFactoryProvider` | 6 | 0 | 0 | 0 | 0 |
| `JsonConfigBasedBoxClientService` | 4 | 3 | 0 | 0 | 0 |
| `JsonPathReader` | 6 | 1 | 0 | 1 | 0 |
| `JsonRecordSetWriter` | 11 | 3 | 1 | 1 | 0 |
| `JsonTreeReader` | 10 | 1 | 0 | 1 | 0 |
| `LoggingRecordSink` | 2 | 0 | 0 | 0 | 0 |
| `MongoDBControllerService` | 2 | 1 | 1 | 0 | 0 |
| `MongoDBLookupService` | 11 | 0 | 0 | 0 | 0 |
| `PropertiesFileLookupService` | 1 | 0 | 0 | 0 | 0 |
| `RedisDistributedMapCacheClientService` | 2 | 0 | 0 | 0 | 0 |
| `RestLookupService` | 10 | 3 | 0 | 0 | 0 |
| `ScriptedLookupService` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedReader` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedRecordSetWriter` | 0 | 0 | 0 | 1 | 0 |
| `ScriptedRecordSink` | 0 | 0 | 0 | 1 | 0 |
| `SimpleCsvFileLookupService` | 4 | 0 | 0 | 0 | 0 |
| `SimpleDatabaseLookupService` | 6 | 0 | 0 | 0 | 0 |
| `SimpleRedisDistributedMapCacheClientService` | 2 | 0 | 0 | 0 | 0 |
| `SimpleScriptedLookupService` | 0 | 0 | 0 | 1 | 0 |
| `SiteToSiteReportingRecordSink` | 2 | 1 | 4 | 0 | 0 |
| `SmbjClientProviderService` | 9 | 1 | 0 | 0 | 0 |
| `StandardAsanaClientProviderService` | 3 | 0 | 0 | 0 | 0 |
| `StandardAzureCredentialsControllerService` | 2 | 0 | 0 | 0 | 0 |
| `StandardDropboxCredentialService` | 4 | 0 | 0 | 0 | 0 |
| `StandardFileResourceService` | 1 | 0 | 0 | 0 | 0 |
| `StandardHashiCorpVaultClientService` | 1 | 0 | 0 | 0 | 0 |
| `StandardOauth2AccessTokenProvider` | 14 | 1 | 0 | 0 | 0 |
| `StandardPGPPrivateKeyService` | 3 | 0 | 0 | 0 | 0 |
| `StandardPGPPublicKeyService` | 2 | 0 | 0 | 0 | 0 |
| `StandardPrivateKeyService` | 3 | 0 | 0 | 0 | 0 |
| `StandardProxyConfigurationService` | 6 | 0 | 0 | 0 | 0 |
| `StandardRestrictedSSLContextService` | 2 | 0 | 0 | 0 | 0 |
| `StandardS3EncryptionService` | 1 | 2 | 2 | 0 | 0 |
| `StandardSSLContextService` | 2 | 0 | 0 | 0 | 0 |
| `StandardWebClientServiceProvider` | 6 | 1 | 0 | 0 | 0 |
| `Syslog5424Reader` | 1 | 0 | 0 | 0 | 0 |
| `SyslogReader` | 1 | 0 | 0 | 0 | 0 |
| `UDPEventRecordSink` | 4 | 0 | 0 | 0 | 0 |
| `VolatileSchemaCache` | 1 | 0 | 0 | 0 | 0 |
| `XMLFileLookupService` | 1 | 0 | 0 | 0 | 0 |
| `XMLReader` | 11 | 1 | 0 | 1 | 0 |
| `XMLRecordSetWriter` | 14 | 2 | 1 | 1 | 0 |
| `YamlTreeReader` | 10 | 3 | 0 | 1 | 0 |

## Renames recovered by hand

Matched renames are found automatically on display name, then description, and only 1:1 in both directions. A rename that changed key *and* display name *and* description defeats all of that and lands in the two 'only on one line' buckets, where it reads as a property that cannot cross when in fact it can. These 19 pairs were mined out of those buckets and confirmed by hand (same allowable set, same default, same required-ness and sensitivity, same dependencies, same ordinal, description still saying the same thing). niflow **translates them for you**, processors and controller services alike — they are counted as renames in the totals above, not as unsupported.

| Type | 2.7.2 key | 1.24.0 key |
|---|---|---|
| `ADLSCredentialsControllerService` | `Account Key` | `storage-account-key` |
| `ConsumeMQTT` | `Connection Timeout` | `Connection Timeout (seconds)` |
| `ConsumeMQTT` | `Keep Alive` | `Keep Alive Interval (seconds)` |
| `DBCPConnectionPool` | `Maximum Connection Lifetime` | `dbcp-max-conn-lifetime` |
| `DeleteDynamoDB` | `Batch Items Per Request` | `Batch items for each request (between 1 and 50)` |
| `ExecuteSQL` | `SQL Query` | `SQL select query` |
| `ExecuteSQLRecord` | `SQL Query` | `SQL select query` |
| `FetchFile` | `Permission Denied Log Level` | `Log level when permission denied` |
| `GetDynamoDB` | `Batch Items Per Request` | `Batch items for each request (between 1 and 50)` |
| `GetDynamoDB` | `Json Document` | `Json Document attribute` |
| `ListenTrapSNMP` | `USM Users Input Method` | `snmp-usm-users-source` |
| `PublishMQTT` | `Connection Timeout` | `Connection Timeout (seconds)` |
| `PublishMQTT` | `Keep Alive` | `Keep Alive Interval (seconds)` |
| `PutDatabaseRecord` | `Database Name` | `put-db-record-catalog-name` |
| `PutDynamoDB` | `Batch Items Per Request` | `Batch items for each request (between 1 and 50)` |
| `PutDynamoDB` | `Json Document` | `Json Document attribute` |
| `PutKinesisStream` | `Max Message Buffer Size` | `max-message-buffer-size` |
| `PutSNS` | `Amazon Resource Name` | `Amazon Resource Name (ARN)` |
| `QueryAirtableTable` | `Personal Access Token` | `api-key` |

Curated in `niflow/processors/rules.py` (`CURATED_TYPE_RENAMES`); add to it and re-run `make version-map` to fold new pairs into the map.

## Possible renames — verify before trusting

These pairs are plausible but not certain, so niflow does **not** translate them: it still reports the key as one that cannot land, which is the safe answer. Every one is either a 1.x property that 2.x split in two (translating would have to pick a half) or a pair whose meaning moved with its name. If one of them matters to you, confirm it on your own servers and move it into `CURATED_TYPE_RENAMES`.

* **ListenSyslog** — `TCP Port` (2.7.2) ≟ `Port` (1.24.0): 1.x had one 'Port' + a Protocol property; 2.x split it into 'TCP Port' and 'UDP Port' (identical descriptions), so one old key maps to two new ones and only the configured protocol says which.
* **ListenSyslog** — `UDP Port` (2.7.2) ≟ `Port` (1.24.0): the other half of the same split.
* **ListenSyslog** — `Worker Threads` (2.7.2) ≟ `Max Number of TCP Connections` (1.24.0): same default (2) and same ordinal, but 'threads that decode messages' and 'concurrent TCP connections accepted' are not the same quantity — carrying a tuned value across would be a guess.
* **StandardS3EncryptionService** — `KMS Key ID` (2.7.2) ≟ `key-id-or-key-material` (1.24.0): 1.x 'Key ID or Key Material' served both roles; 2.x split it into 'KMS Key ID' and 'Key Material'. Which half a value belongs to depends on the encryption strategy, so niflow will not choose for you.
* **StandardS3EncryptionService** — `Key Material` (2.7.2) ≟ `key-id-or-key-material` (1.24.0): the other half of the same split.
* **IdentifyMimeType** — `Custom MIME Configuration` (2.7.2) ≟ `config-body` (1.24.0): 2.x folded 1.x's 'Config Body' and 'Config File' into one property that takes a URL, a path, or the config text, gated by the new 'Config Strategy'. A body translates cleanly; a file path does not, and the strategy has to be set either way.

## What this map cannot tell you

The harvest reads NiFi's own property *descriptors*. That makes it exact
about names, allowable values, required-ness and defaults — and blind to
everything else:

* **Behavioural drift.** A property present on both lines under the same
  name, with the same allowable values, can still *mean* something
  different in the engine (parsing, rounding, retry semantics, what an
  empty value implies). Nothing in the descriptor exposes that, so this
  map reports such a property as identical. It is the largest known gap.
  One confirmed instance sits in the curated renames above:
  `DBCPConnectionPool`'s `Maximum Connection Lifetime` is the same property
  as 1.x's `dbcp-max-conn-lifetime` (same default), but the 1.x description
  says *milliseconds* where 2.x describes a duration — the key translates,
  the value may need re-reading.
* **Relationship and attribute changes.** Only properties are diffed here.
* **Undetected renames.** Renames are matched on display name, then
  description, and only when the pairing is 1:1 in both directions —
  deliberately conservative, because a wrong pairing would silently write
  a value to the wrong property. A rename that changed *both* key and
  display name and description defeats every automatic signal and shows up
  as one `only_new` plus one `only_old` entry. Those buckets have since
  been mined by hand — see *Renames recovered by hand* above for the pairs
  now translated, and *Possible renames* for the ones left for you to
  confirm — but the mining was done against these two servers only, so a
  regenerated map may surface fresh ones.
* **Types that would not instantiate.** Restricted or dependency-hungry
  types are skipped by the harvest and appear in neither table.
* **Your NARs, not ours.** This was harvested from stock Apache NiFi
  2.7.2 and 1.24.0 containers. A work server with extra NARs (or a
  1.28 rather than 1.24) differs; re-run `make version-map` pointed at
  the real pair to get a map that matches your estate.

