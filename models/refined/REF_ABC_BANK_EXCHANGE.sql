WITH
current_from_history as (
  {{ current_from_history(
    history_rel = ref('HIST_ABC_BANK_EXCHANGE'),
    key_column = 'EXCHANGE_HKEY',
    ) }}
)
SELECT *
FROM current_from_history