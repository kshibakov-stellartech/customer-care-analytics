# Disputes Reconciliation Report

- Provider: **PayPal**
- Merchant Account: **stellartech LTD**
- Source schema: `fivetran_paypal_prod`
- Tables used: `dispute`, `disputed_transaction`, `adjudication`, `evidence`, `dispute_message`, `money_movement`
- Scope: 3 disputes
  - `PP-R-AYO-596018258`
  - `PP-R-NOA-596248579`
  - `PP-R-UPU-596345837`
- Generated on: **2026-05-07**

## 1) Matching manual export vs DB (case-by-case)

### Case 1: `PP-R-AYO-596018258`
- Manual: `Case Type=Chargeback`, `Reason=Merchandise`, `Status=Win`, `Amount=24.98 USD`, `Evidence submitted=FALSE`
- DB:
  - `create_time=2025-10-01 05:03:29.184 UTC`
  - `amount=24.98`, `currency_code=USD`
  - `reason=MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED`
  - `life_cycle_stage=PRE_ARBITRATION`, `channel=EXTERNAL`
  - `status=RESOLVED`, `outcome_code=RESOLVED_SELLER_FAVOUR`
- Conclusion:
  - `Chargeback` mapping is correct (external chargeback lifecycle).
  - `Merchandise` meaning matches `MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED`.
  - `Win` mapping is correct via `RESOLVED_SELLER_FAVOUR`.

### Case 2: `PP-R-NOA-596248579`
- Manual: `Case Type=Dispute`, `Reason=Item not received`, `Status=Refund`, `Amount=9.99 USD`, `Evidence submitted=FALSE`
- DB:
  - `create_time=2025-10-02 16:26:51.667 UTC`
  - `amount=9.99`, `currency_code=USD`
  - `reason=MERCHANDISE_OR_SERVICE_NOT_RECEIVED`
  - `life_cycle_stage=INQUIRY`, `channel=INTERNAL`
  - `status=RESOLVED`, `outcome_code=RESOLVED_BUYER_FAVOUR`
  - evidence action contains `ACCEPT_CLAIM` with response option `REFUND`
- Conclusion:
  - `Dispute` mapping is correct (`INQUIRY`/`INTERNAL`).
  - `Item not received` mapping is correct.
  - `Refund` mapping is correct (seller accepted claim + refund flow).

### Case 3: `PP-R-UPU-596345837`
- Manual: `Case Type=Chargeback`, `Reason=Unauthorized`, `Status=Loss`, `Amount=40.8 USD`, `Evidence submitted=FALSE`
- DB:
  - `create_time=2025-10-03 09:04:12.522 UTC`
  - `amount=40.8`, `currency_code=USD`
  - `reason=UNAUTHORISED`
  - `life_cycle_stage=CHARGEBACK`, `channel=EXTERNAL`
  - `status=RESOLVED`, `outcome_code=RESOLVED_BUYER_FAVOUR`
- Conclusion:
  - `Chargeback` mapping is correct.
  - `Unauthorized` mapping is correct.
  - `Loss` mapping is correct for merchant view (`RESOLVED_BUYER_FAVOUR`).

## 2) Timeline / change log by dispute (UTC)

### `PP-R-AYO-596018258`
1. `2025-10-01 05:03:29` - dispute created.
2. `2025-10-10 22:52:29` - adjudication (`CHARGEBACK`, `RECOVER_FROM_SELLER`).
3. `2025-10-10 22:57:41` - money movement: seller `DEBIT 24.98` (`DISPUTE_SETTLEMENT`).
4. `2025-10-10 22:57:41` - money movement: seller `DEBIT 1.4` (`DISPUTE_SETTLEMENT_FEE`).
5. `2025-10-17 05:05:23` - adjudication (`PRE_ARBITRATION`, `RECOVER_FROM_SELLER`).
6. `2025-12-31 06:27:38` - money movement: seller `CREDIT 24.98` (`DISPUTE_SETTLEMENT`).
7. `2025-12-31 06:28:50` - dispute updated/finalized (`RESOLVED_SELLER_FAVOUR`).

### `PP-R-NOA-596248579`
1. `2025-10-02 16:26:51` - dispute created (`INQUIRY`).
2. `2025-10-02 16:26:51` - buyer message: "I want a refund.".
3. `2025-10-03` - seller evidence/action: `ACCEPT_CLAIM` + `REFUND`.
4. `2025-10-03 16:49:53` - money movement: buyer `CREDIT 9.99`.
5. `2025-10-03 16:49:53` - money movement: seller `DEBIT 9.99`.
6. `2025-10-03 16:53:16` - dispute updated/finalized (`RESOLVED_BUYER_FAVOUR`).

### `PP-R-UPU-596345837`
1. `2025-10-03 09:04:12` - dispute created (`CHARGEBACK`).
2. `2025-10-05` - seller evidence/action: `ACCEPT_CLAIM` + `REFUND`.
3. `2025-10-05 11:00:05` - money movement: seller `DEBIT 40.8`.
4. `2025-10-05 11:00:05` - money movement: buyer `CREDIT 40.8`.
5. `2025-12-27 12:30:40` - dispute updated/finalized (`RESOLVED_BUYER_FAVOUR`).

## 3) PayPal taxonomy and mapping to manual export

### Case Type taxonomy
- `Dispute` (manual) -> usually `life_cycle_stage=INQUIRY`, `channel=INTERNAL`.
- `Chargeback` (manual) -> usually `life_cycle_stage IN (CHARGEBACK, PRE_ARBITRATION, ARBITRATION)`, `channel=EXTERNAL`.

### Reason taxonomy (observed)
- `MERCHANDISE_OR_SERVICE_NOT_AS_DESCRIBED` -> `Merchandise`.
- `MERCHANDISE_OR_SERVICE_NOT_RECEIVED` -> `Item not received`.
- `UNAUTHORISED` -> `Unauthorized`.

### Status taxonomy (merchant-oriented)
- `RESOLVED_SELLER_FAVOUR` -> `Win`.
- `RESOLVED_BUYER_FAVOUR` + seller refund acceptance flow (`ACCEPT_CLAIM/REFUND`) -> `Refund`.
- `RESOLVED_BUYER_FAVOUR` without seller-favor outcome -> `Loss`.

### Process roadmaps
- `INQUIRY` path: open dispute -> buyer/seller messages/evidence -> accept claim/refund or escalation -> resolved.
- `CHARGEBACK/PRE_ARBITRATION` path: external card dispute -> adjudication + funds movements (debit/fee/reversal possible) -> resolved.

## 4) Important data-quality note

Manual export says `Evidence submitted=FALSE` for all 3 disputes.
In DB, `evidence` table has rows for all 3 disputes, including:
- seller evidence (`PROOF_OF_FULFILLMENT`) for `PP-R-AYO-596018258`
- seller `ACCEPT_CLAIM/REFUND` actions for two cases

Interpretation: manual flag likely tracks a narrower evidence definition than raw `evidence` table events.
