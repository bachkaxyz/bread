-- each buy storage event has two transfers:
-- from user -> jkl1fx39l8lwltl7etg0e5cv2rwfxvw0lz64uw8h4e
-- (jkl1fx39 = transfer_sender[0] and message_sender[1])
-- jkl1fx39 -> jkl1t35eusvx97953uk47r3z4ckwd2prkn3fay76r8 (storage module account
-- (jkl1t35 = transfer_receiver[1])
--
-- this storage module account then distributes the funds as rewards to each of the
-- storage providers
--
-- here we care about when a user buys storage, and how much they buy
select
    logs.txhash,
    txs.timestamp,
    message_sender#>>'{0}' as message_sender, 
    (
        regexp_matches(
            (parsed -> 'transfer_amount')#>>'{0}', '[0-9]*'
        )
    )[1]::numeric as transfer_amount,
    (
        regexp_matches(
            (parsed -> 'transfer_amount')#>>'{0}',
            '[\D]+[_a-zA-Z0-9]*'
        )
    )[1] as transfer_denom
from {{ source("indexer", "logs") }} left join {{ source("indexer", "txs")}} on logs.txhash = txs.txhash
where parsed -> 'message_action' ? 'buy_storage'
