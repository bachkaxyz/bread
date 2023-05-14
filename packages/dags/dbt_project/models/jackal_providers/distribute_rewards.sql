-- jkl1t35eusvx97953uk47r3z4ckwd2prkn3fay76r8 is the storage module address

select logs.txhash, txs.timestamp, logs.parsed -> 'transfer_receiver'
from {{ source("indexer", "logs") }}
left join {{ source("indexer", "txs") }} on logs.txhash = txs.txhash
where parsed -> 'transfer_sender' ? 'jkl1t35eusvx97953uk47r3z4ckwd2prkn3fay76r8'
order by txs.timestamp desc
