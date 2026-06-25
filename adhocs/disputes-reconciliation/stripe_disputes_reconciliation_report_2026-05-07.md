# Stripe Disputes Reconciliation Report

Дата отчета: 2026-05-07  
Провайдер: **Stripe**  
Мерчант-аккаунт (из ручной выгрузки): **SmartyMe**

## 1) Scope и источники

Проверены 2 диспьюта из ручной выгрузки:
- `du_1SDdu0HFFv8NmmxdBfbAuUjf`
- `du_1SDfuoHFFv8NmmxdOGVwOO9F`

Сверка выполнена по таблицам схемы `data_bronze_stripe_prod`:
- `stripe_charge_dispute_created`
- `stripe_charge_dispute_updated`
- `stripe_charge_dispute_closed`
- `stripe_charge_dispute_funds_withdrawn`

## 2) Reconciliation: ручная выгрузка vs БД

### Dispute 1: `du_1SDdu0HFFv8NmmxdBfbAuUjf`

Ручная выгрузка:
- `case_type`: `chargeback`
- `reason`: `fraudulent`
- `status`: `needs_response`
- `evidence_submitted`: `FALSE`
- `amount`: `84.33 USD`

Факт по БД (timeline):
1. `2025-10-02 04:14:55 UTC` — `charge.dispute.created`, статус `needs_response`
2. `2025-10-02 04:14:56 UTC` — `charge.dispute.funds_withdrawn`, статус `lost`
3. `2025-10-02 04:14:57 UTC` — `charge.dispute.closed`, статус `lost`

Итог сверки:
- `reason`, `amount`, `currency` совпадают.
- `evidence_submitted=FALSE` согласуется с `has_evidence=false`.
- `status` в ручной выгрузке **неактуален** (исторический): актуальный финальный статус в БД — **`lost`**.

### Dispute 2: `du_1SDfuoHFFv8NmmxdOGVwOO9F`

Ручная выгрузка:
- `case_type`: `chargeback`
- `reason`: `product_not_received`
- `status`: `under_review`
- `evidence_submitted`: `TRUE`
- `amount`: `40.8 USD`

Факт по БД (timeline):
1. `2025-10-02 06:31:15 UTC` — `charge.dispute.created`, статус `needs_response`
2. `2025-10-03 23:54:18 UTC` — `charge.dispute.updated` (служебный апдейт)
3. `2025-10-18 18:04:26 UTC` — `charge.dispute.updated` (служебный апдейт)
4. `2025-10-18 18:04:44 UTC` — `charge.dispute.updated` (служебный апдейт)
5. `2025-10-18 18:05:18 UTC` — `charge.dispute.updated` (служебный апдейт)
6. `2025-10-18 18:06:16 UTC` — `charge.dispute.updated`, переход `needs_response -> under_review`
7. `2026-01-02 20:00:18 UTC` — `charge.dispute.closed`, статус `lost`

Итог сверки:
- `reason`, `amount`, `currency` совпадают.
- `evidence_submitted=TRUE` согласуется с `has_evidence=true` и `submission_count=1`.
- `status` в ручной выгрузке **промежуточный** (`under_review`), но не финальный: актуальный финальный статус в БД — **`lost`**.

## 3) Сводный вывод по актуальности ручной выгрузки

По обоим проверенным кейсам ручная выгрузка содержит не финальное состояние `status`:
- `du_1SDdu0HFFv8NmmxdBfbAuUjf`: выгрузка `needs_response`, факт `lost`
- `du_1SDfuoHFFv8NmmxdOGVwOO9F`: выгрузка `under_review`, факт `lost`

Следовательно, ручную выгрузку нельзя использовать как reliable latest snapshot без дополнительного пересчета "последнего события" по dispute timeline.

## 4) Таксономия Stripe disputes (для маппинга и мониторинга)

### 4.1 Типы кейсов
- `case_type=chargeback` — карточный спор, инициированный банком-эмитентом через платежную сеть.

### 4.2 Основные reason (в проверенных кейсах)
- `fraudulent` — держатель карты заявляет мошенничество.
- `product_not_received` — товар/услуга не получены.

### 4.3 События lifecycle (webhook/event level)
- `charge.dispute.created` — кейс создан.
- `charge.dispute.updated` — изменения кейса (status/evidence/прочие атрибуты).
- `charge.dispute.funds_withdrawn` — списание спорной суммы (и/или движение средств по кейсу).
- `charge.dispute.closed` — финализация кейса.

### 4.4 Статусный roadmap (обобщенно)
1. `needs_response` — требуется ответ/доказательства мерчанта.
2. `under_review` — доказательства поданы, кейс на рассмотрении.
3. Финал после `closed`: обычно `won` или `lost`.

В проверенных двух кейсах финальный исход: `lost`.

## 5) Маппинг полей ручной выгрузки к данным БД

- `Provider` -> константа провайдера (`Stripe`)
- `Merchant Account` -> внешний бизнес-атрибут (в bronze Stripe обычно не хранится как отдельное поле кейса)
- `Created Date` -> `from_unixtime(data__object__created)` (дата создания dispute object)
- `Dispute Id` -> `data__object__id`
- `Case Type` -> для этих кейсов `chargeback`
- `Reason` -> `data__object__reason`
- `Status` -> `data__object__status` из **latest event**
- `Evidence submitted` -> `data__object__evidence_details__has_evidence`
- `Amount` -> `data__object__amount / 100`
- `Currency` -> `upper(data__object__currency)`
- `USD amount` -> для USD совпадает с `Amount`, для не-USD нужна FX-нормализация

## 6) Рекомендация для production-выгрузки

Для корректного "текущего состояния" спора:
- строить snapshot по последнему событию на `dispute_id` (ordered by `event_created_ts`, tie-breaker `_ingested_at`);
- хранить отдельно immutable event log;
- делать дедуп событий по устойчивому ключу (`event_id` или эквивалентному idempotency key).

