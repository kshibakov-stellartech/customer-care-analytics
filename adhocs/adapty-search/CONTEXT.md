# Adapty Search — Контекст

Утилита: поиск пользователя по email в Adapty (данные подписок SmartyMe).

## Файлы

| Файл | Назначение |
|------|-----------|
| `adapty_search_by_email.sql` | Поиск профиля и подписок пользователя по email в Adapty-таблицах |

## Источники данных

- `data_bronze_supabase_prod.smartyme_public__subscriptions` — подписки из Adapty

## Использование

Подставить email в запрос, выполнить в Athena. Возвращает профиль, статус подписки, историю.
