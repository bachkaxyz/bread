-- each buy storage event has two transfers:
-- from user -> jkl1fx39l8lwltl7etg0e5cv2rwfxvw0lz64uw8h4e ->
-- jkl1t35eusvx97953uk47r3z4ckwd2prkn3fay76r8 (storage module account)

-- this storage module account then distributes the funds as rewards to  storage providers

-- so here we care about when a user buys storage, and how much they buy
select
    logs.txhash,
    txs.timestamp,
    message_sender  # >>'{0}' as message_sender, 
    (regexp_matches(jsonb_array_elements_text(parsed -> 'transfer_amount'), '[0-9]*'))[
        1
    ]::numeric as ta
from indexer.logs
left join indexer.txs on logs.txhash = txs.txhash
where parsed -> 'message_action' ? 'buy_storage'
