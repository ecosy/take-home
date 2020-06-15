SELECT to_address FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
where token_address IN 
  ('0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d',  -- Decentraland LAND 
  '0x959e104e1a4db6317fa58f8295f586e1a978c297',   -- Decentraland estate
  '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0',   -- SuperRare
  '0xdceaf1652a131F32a821468Dc03A92df0edd86Ea',   -- MCH Extension
  '0x273f7f8e6489682df756151f5525576e322d51a3',   -- MCH Heros
  '0x617913dd43dbdf4236b85ec7bdf9adfd7e35b340'   -- MCH Land
  )
group by to_address