with
    ibc_transfers_out as (
        select
            logs.txhash,
            logs.msg_index,
            txs.timestamp,
            (
                regexp_matches(
                    jsonb_array_elements_text(parsed -> 'transfer_amount'), '[0-9]*'
                )
            )[1]::numeric as transfer_amount,
            (
                regexp_replace(
                    jsonb_array_elements_text(parsed -> 'transfer_amount'), '[0-9]*', ''
                )
            ) as transfer_denom,
            jsonb_array_elements_text(parsed -> 'ibc_transfer_sender') as ibc_sender,
            jsonb_array_elements_text(parsed -> 'ibc_transfer_receiver') as ibc_recv,
            jsonb_array_elements_text(
                parsed -> 'send_packet_packet_src_port'
            ) as src_port,
            jsonb_array_elements_text(
                parsed -> 'send_packet_packet_src_channel'
            ) as src_channel,
            jsonb_array_elements_text(
                parsed -> 'send_packet_packet_dst_port'
            ) as dst_port,
            jsonb_array_elements_text(
                parsed -> 'send_packet_packet_dst_channel'
            ) as dst_channel
        from {{ source("indexer", "logs") }}
        left join {{ source("indexer", "txs") }} on logs.txhash = txs.txhash
        where
            (parsed -> 'message_module' ? 'transfer')
            and (parsed -> 'message_module' ? 'ibc_channel')
    )
select *
from ibc_transfers_out
