# Disputes Data Model (Stripe / PayPal / Adyen)

Дата: 2026-05-04
Источник: открытая документация провайдеров (API/docs/webhooks)

## Что важно понимать заранее

Провайдеры обычно не раскрывают внутреннюю физическую схему БД (таблицы/индексы/партиции/инжест пайплайны).
Из открытых источников хорошо видны:
- доменные сущности;
- lifecycle/статусы;
- API-операции;
- webhook-события;
- поля для аналитического выгруза.

## Общая модель (почти у всех)

- Есть сущность `Dispute` / `Case` с уникальным идентификатором.
- Есть связь с исходным платежом (`charge` / `payment` / `transaction`).
- Есть набор статусов и стадий жизненного цикла.
- Есть evidence/документы и действия (accept/defend/submit evidence/appeal).
- Изменения приходят через API и webhooks.
- Для DWH-инжеста практикуется idempotent upsert по `dispute_id` (+ хранение истории событий).

---

## Stripe

### Сущности
- `Dispute` (Payments disputes), привязка к `charge` и/или `payment_intent`.
- Поля: `reason`, `status`, `amount`, `currency`, `evidence`, `balance_transactions`, `created`.

### Обновления и поведение
- Update dispute/evidence делается через API.
- Важно: при обновлении evidence в hash Stripe сабмитит на review весь evidence hash.
- События: например `charge.dispute.created` (и дальнейшие статусы).

### Аналитическое хранение (публично видимое)
- В Stripe Sigma/Data Pipeline есть таблица `disputes`.
- Документация явно описывает: одна строка = один Dispute object.
- Можно join с `charges` по `disputes.charge_id = charges.id`.

---

## PayPal

### Сущности
- Dispute case (`dispute_id`) + связанные `disputed_transactions`.
- Поля: `status`, `dispute_lifecycle_stage`, `dispute_channel`, `dispute_amount`, `create_time`, `update_time`.
- Дополнительно: `messages`, `evidences`, `fund_movements`, `supporting_info`.

### Lifecycle
- Явные стадии: `INQUIRY`, затем claim-стадии (`CHARGEBACK`, `PRE_ARBITRATION`, `ARBITRATION`), затем `RESOLVED`.
- Доступные действия на кейсе зависят от стадии и выдаются через HATEOAS links.

### Обновления
- Webhooks: `CUSTOMER.DISPUTE.CREATED`, `CUSTOMER.DISPUTE.UPDATED`, `CUSTOMER.DISPUTE.RESOLVED`.
- Рабочий паттерн: webhook -> fetch case details -> upsert + audit trail по изменениям.

---

## Adyen

### Сущности и ключи
- Два ключевых reference в dispute webhooks:
  - `pspReference` — идентификатор dispute;
  - `originalReference` — идентификатор исходного платежа.
- Поля события: `eventCode`, `eventDate`, `amount`, `reason`, `additionalData.disputeStatus`.

### Lifecycle (event-driven)
- Типовые `eventCode`: `NOTIFICATION_OF_CHARGEBACK`, `CHARGEBACK`, `INFORMATION_SUPPLIED`, `CHARGEBACK_REVERSED`, `SECOND_CHARGEBACK` и др.
- У разных payment methods есть вариации флоу, но event-модель общая.

### API для работы с кейсом
- Disputes API: retrieve applicable defense reasons, supply/delete defense document, accept/defend dispute.

---

## Практический вывод для хранилища (vendor-agnostic)

Минимально разумная модель в DWH:
- `fact_disputes` (current snapshot по dispute: status, amount, reason, provider IDs, payment link);
- `dispute_events` (immutable timeline: provider_event_type, event_time, payload hash, raw payload);
- `dispute_evidence` (документы/типы/время загрузки/кто запросил);
- `dispute_funds_movements` (withdraw/reversal/fees если доступны);
- `dim_dispute_status_map` (нормализация Stripe/PayPal/Adyen статусов в внутренний canonical status).

Ключевые инженерные правила:
- idempotency по event ID или детерминированному ключу (`provider + dispute_id + event_type + event_time`);
- late-arriving events handling;
- хранение raw payload для reprocessing;
- SCD/история изменения статусов (не только последний snapshot).

---

## Источники

### Stripe
- https://docs.stripe.com/api/disputes
- https://docs.stripe.com/api/disputes/update
- https://docs.stripe.com/disputes/responding
- https://docs.stripe.com/stripe-data/query-disputes-and-fraud-data
- https://docs.stripe.com/stripe-data/sigma

### PayPal
- https://developer.paypal.com/docs/api/customer-disputes/v1/
- https://developer.paypal.com/docs/multiparty/disputes-chargebacks/dispute-lifecycle/
- https://developer.paypal.com/docs/multiparty/disputes-chargebacks/webhooks/
- https://developer.paypal.com/docs/disputes/disputes-reference/

### Adyen
- https://docs.adyen.com/risk-management/disputes-api/dispute-notifications
- https://docs.adyen.com/risk-management/disputes-api/disputes-api-reference
- https://docs.adyen.com/api-explorer/Disputes/30/overview

