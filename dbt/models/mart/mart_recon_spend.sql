{{
  config(
    materialized = 'table',
    alias = var('company') ~ '_table_recon_all_all_recon_spend',
    cluster_by = [
      'budget_group',
      'category_level_1',
      'track',
    ],
    tags = ['mart', 'recon', 'spend']
  )
}}

select
    *
from {{ ref('int_recon_spend') }}